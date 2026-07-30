// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/geo/geometry-wrapper-wkb.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <limits>
#include <sstream>
#include <variant>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <gutil/strings/numbers.h>
#include <gutil/strings/split.h>

#include "exprs/anyval-util.h"
#include "exprs/geo/wkb-format.h"
#include "exprs/geo/wkb-serialization.h"
#include "exprs/geo/wkt.h"
#include "udf/udf.h"
#include "util/gflag-validator-util.h"
#include "util/string-parser.h"

#include "common/names.h"

DEFINE_int32(geospatial_max_quad_segs, 999999, "Maximum allowed value for number of "
    "quadrant segments parameters in geospatial functions.");
DEFINE_validator(geospatial_max_quad_segs, ge_one);

namespace impala::geo {

using impala_udf::BooleanVal;
using impala_udf::DoubleVal;

bool GeometryWrapperWkb::FromWkb(const StringVal& geom) {
  if (geom.is_null || geom.len < WKB_HEADER_SIZE) return false;
  uint8_t byte_order = geom.ptr[WKB_BYTE_ORDER_OFFSET];
  if (byte_order != 0x01 && byte_order != 0x00) return false;
  bool swap = (byte_order == 0x00);
  uint32_t wkb_type = GetWkbType(geom);
  ogc_type_ = WkbTypeToOgcType(wkb_type);
  if (ogc_type_ == UNKNOWN) return false;

  switch (ogc_type_) {
    case ST_POINT:
      return ReadWkbPoint(geom.ptr, geom.len, point_, swap);
    case ST_LINESTRING:
      return ReadWkbLineString(geom.ptr, geom.len, linestring_, swap);
    case ST_POLYGON:
      return ReadWkbPolygon(geom.ptr, geom.len, polygon_, swap);
    case ST_MULTIPOINT:
      return ReadWkbMultiPoint(geom.ptr, geom.len, multipoint_, swap);
    case ST_MULTILINESTRING:
      return ReadWkbMultiLineString(geom.ptr, geom.len, multi_linestring_, swap);
    case ST_MULTIPOLYGON:
      return ReadWkbMultiPolygon(geom.ptr, geom.len, multi_polygon_, swap);
    default:
      return false;
  }
}

bool GeometryWrapperWkb::FromWkt(FunctionContext* ctx, const StringVal& wkt,
    OGCType type) {
  ogc_type_ = type;
  std::string s = AnyValUtil::ToString(wkt);
  try {
    switch (type) {
      case ST_POINT:
        return fromWkt(s, point_);
      case ST_LINESTRING:
        return fromWkt(s, linestring_);
      case ST_POLYGON:
        return fromWkt(s, polygon_);
      case ST_MULTIPOINT:
        return fromWkt(s, multipoint_);
      case ST_MULTILINESTRING:
        return fromWkt(s, multi_linestring_);
      case ST_MULTIPOLYGON:
        return fromWkt(s, multi_polygon_);
      default:
        ctx->SetError("Unsupported geometry type for WKT parsing");
        return false;
    }
  } catch (bg::exception& ex) {
    ctx->SetError(ex.what());
    return false;
  }
}

bool GeometryWrapperWkb::FromCoordinates(FunctionContext* ctx, OGCType type,
    int num_coords, const double* coords) {
  if (num_coords == 0 || num_coords % 2 != 0) {
    ctx->SetError("Invalid number of coordinates");
    return false;
  }
  ogc_type_ = type;

  auto readPoints = [&](auto& container) {
    container.clear();
    container.reserve(num_coords / 2);
    for (int i = 0; i < num_coords; i += 2) {
      container.emplace_back(coords[i], coords[i + 1]);
    }
  };

  switch (type) {
    case ST_LINESTRING:
      readPoints(linestring_);
      return true;
    case ST_MULTIPOINT:
      readPoints(multipoint_);
      return true;
    case ST_POLYGON:
      readPoints(polygon_.outer());
      bg::correct(polygon_);
      return true;
    default:
      ctx->SetError("Geometry type not supported for coordinate construction");
      return false;
  }
}

StringVal GeometryWrapperWkb::ToWkb(FunctionContext* ctx) const {
  switch (ogc_type_) {
    case ST_POINT:
      return WriteWkbPoint(ctx, point_);
    case ST_LINESTRING:
      return WriteWkbLineString(ctx, linestring_);
    case ST_POLYGON:
      return WriteWkbPolygon(ctx, polygon_);
    case ST_MULTIPOINT:
      return WriteWkbMultiPoint(ctx, multipoint_);
    case ST_MULTILINESTRING:
      return WriteWkbMultiLineString(ctx, multi_linestring_);
    case ST_MULTIPOLYGON:
      return WriteWkbMultiPolygon(ctx, multi_polygon_);
    default:
      return StringVal::null();
  }
}

void GeometryWrapperWkb::appendDouble(double val) {
  size_t pos = wkt_buf_.size();
  wkt_buf_.resize(pos + kDoubleToBufferSize);
  DoubleToBuffer(val, &wkt_buf_[pos]);
  wkt_buf_.resize(pos + strlen(&wkt_buf_[pos]));
}

void GeometryWrapperWkb::appendCoord(double x, double y) {
  appendDouble(x);
  wkt_buf_ += ' ';
  appendDouble(y);
}

void GeometryWrapperWkb::appendRing(
    const bg::model::ring<point2d, true>& ring, bool reverse) {
  wkt_buf_ += '(';
  size_t n = ring.size();
  for (size_t i = 0; i < n; i++) {
    if (i > 0) wkt_buf_ += ", ";
    size_t idx = reverse ? (n - 1 - i) : i;
    appendCoord(ring[idx].x(), ring[idx].y());
  }
  wkt_buf_ += ')';
}

void GeometryWrapperWkb::appendPolygonBody(const polygon2d& poly) {
  wkt_buf_ += '(';
  appendRing(poly.outer(), true);
  for (const auto& inner : poly.inners()) {
    wkt_buf_ += ", ";
    appendRing(inner, true);
  }
  wkt_buf_ += ')';
}

StringVal GeometryWrapperWkb::ToWkt(FunctionContext* ctx) {
  if (ogc_type_ == UNKNOWN) return StringVal::null();

  if (IsEmpty()) {
    const char* prefix = OgcTypeToWktPrefix[ogc_type_];
    size_t prefix_len = strlen(prefix);
    constexpr const char EMPTY_SUFFIX[] = " EMPTY";
    StringVal result(ctx, prefix_len + sizeof(EMPTY_SUFFIX) - 1);
    if (UNLIKELY(result.is_null)) return StringVal::null();
    memcpy(result.ptr, prefix, prefix_len);
    memcpy(result.ptr + prefix_len, EMPTY_SUFFIX, sizeof(EMPTY_SUFFIX) - 1);
    return result;
  }

  wkt_buf_.clear();
  int num_coords = NumPoints();
  wkt_buf_.reserve(32 + num_coords * (2 * kDoubleToBufferSize + 3));

  wkt_buf_ += OgcTypeToWktPrefix[ogc_type_];
  wkt_buf_ += ' ';

  switch (ogc_type_) {
    case ST_POINT:
      wkt_buf_ += '(';
      appendCoord(point_.x(), point_.y());
      wkt_buf_ += ')';
      break;
    case ST_LINESTRING:
      wkt_buf_ += '(';
      for (size_t i = 0; i < linestring_.size(); i++) {
        if (i > 0) wkt_buf_ += ", ";
        appendCoord(linestring_[i].x(), linestring_[i].y());
      }
      wkt_buf_ += ')';
      break;
    case ST_POLYGON:
      appendPolygonBody(polygon_);
      break;
    case ST_MULTIPOINT:
      wkt_buf_ += '(';
      for (size_t i = 0; i < multipoint_.size(); i++) {
        if (i > 0) wkt_buf_ += ", ";
        wkt_buf_ += '(';
        appendCoord(multipoint_[i].x(), multipoint_[i].y());
        wkt_buf_ += ')';
      }
      wkt_buf_ += ')';
      break;
    case ST_MULTILINESTRING:
      wkt_buf_ += '(';
      for (size_t i = 0; i < multi_linestring_.size(); i++) {
        if (i > 0) wkt_buf_ += ", ";
        const auto& ls = multi_linestring_[i];
        wkt_buf_ += '(';
        for (size_t j = 0; j < ls.size(); j++) {
          if (j > 0) wkt_buf_ += ", ";
          appendCoord(ls[j].x(), ls[j].y());
        }
        wkt_buf_ += ')';
      }
      wkt_buf_ += ')';
      break;
    case ST_MULTIPOLYGON:
      wkt_buf_ += '(';
      for (size_t i = 0; i < multi_polygon_.size(); i++) {
        if (i > 0) wkt_buf_ += ", ";
        appendPolygonBody(multi_polygon_[i]);
      }
      wkt_buf_ += ')';
      break;
    default:
      return StringVal::null();
  }

  StringVal result(ctx, wkt_buf_.size());
  if (UNLIKELY(result.is_null)) return StringVal::null();
  memcpy(result.ptr, wkt_buf_.data(), wkt_buf_.size());
  return result;
}

// --- Geometry property methods ---

double GeometryWrapperWkb::Area() const {
  switch (ogc_type_) {
    case ST_POLYGON: return bg::area(polygon_);
    case ST_MULTIPOLYGON: return bg::area(multi_polygon_);
    default: return 0.0;
  }
}

double GeometryWrapperWkb::Length() const {
  switch (ogc_type_) {
    case ST_LINESTRING: return bg::length(linestring_);
    case ST_MULTILINESTRING: return bg::length(multi_linestring_);
    default: return 0.0;
  }
}

namespace {

constexpr const char* BUFFER_STYLE_KEY_QUAD_SEGS = "quad_segs";
constexpr const char* BUFFER_STYLE_KEY_ENDCAP = "endcap";
constexpr const char* BUFFER_STYLE_KEY_MITRE_LIMIT = "mitre_limit";
constexpr const char* BUFFER_STYLE_KEY_MITER_LIMIT = "miter_limit";
constexpr const char* BUFFER_STYLE_KEY_JOIN = "join";
constexpr const char* BUFFER_STYLE_KEY_SIDE = "side";

constexpr const char* BUFFER_STYLE_VALUE_SQUARE = "square";
constexpr const char* BUFFER_STYLE_VALUE_FLAT = "flat";
constexpr const char* BUFFER_STYLE_VALUE_BUTT = "butt";
constexpr const char* BUFFER_STYLE_VALUE_ROUND = "round";
constexpr const char* BUFFER_STYLE_VALUE_MITRE = "mitre";
constexpr const char* BUFFER_STYLE_VALUE_MITER = "miter";
constexpr const char* BUFFER_STYLE_VALUE_BEVEL = "bevel";
constexpr const char* BUFFER_STYLE_VALUE_LEFT = "left";
constexpr const char* BUFFER_STYLE_VALUE_RIGHT = "right";
constexpr const char* BUFFER_STYLE_VALUE_BOTH = "both";

// Taking into account the longest potential value for each key, the maximum length for
// the buffer style string is 378 characters. Use a higher limit that is more round.
constexpr int BUFFER_STYLE_MAX_LEN = 1000;

// Argument indicies.
const uint8_t ARG_DIST_IDX = 1;
const uint8_t ARG_USE_SPHEROID_IDX = 2;
const uint8_t ARG_BUFFER_STYLE_IDX = 3;

template <class LhsT, class RhsT>
double computeDistance(const LhsT& lhs, const RhsT& rhs) {
  return bg::distance(lhs, rhs);
}

template <class LhsT>
double dispatchRhs(const LhsT& lhs, const GeometryWrapperWkb& rhs) {
  switch (rhs.type()) {
    case ST_POINT: return computeDistance(lhs, rhs.point());
    case ST_LINESTRING: return computeDistance(lhs, rhs.linestring());
    case ST_POLYGON: return computeDistance(lhs, rhs.polygon());
    case ST_MULTIPOINT: return computeDistance(lhs, rhs.multipoint());
    case ST_MULTILINESTRING: return computeDistance(lhs, rhs.multi_linestring());
    case ST_MULTIPOLYGON: return computeDistance(lhs, rhs.multi_polygon());
    default: return -1.0;
  }
}
} // anonymous namespace

double GeometryWrapperWkb::Distance(const GeometryWrapperWkb& other) const {
  switch (ogc_type_) {
    case ST_POINT: return dispatchRhs(point_, other);
    case ST_LINESTRING: return dispatchRhs(linestring_, other);
    case ST_POLYGON: return dispatchRhs(polygon_, other);
    case ST_MULTIPOINT: return dispatchRhs(multipoint_, other);
    case ST_MULTILINESTRING: return dispatchRhs(multi_linestring_, other);
    case ST_MULTIPOLYGON: return dispatchRhs(multi_polygon_, other);
    default: return -1.0;
  }
}

int GeometryWrapperWkb::NumPoints() const {
  switch (ogc_type_) {
    case ST_POINT: return bg::num_points(point_);
    case ST_LINESTRING: return bg::num_points(linestring_);
    case ST_POLYGON: return bg::num_points(polygon_);
    case ST_MULTIPOINT: return bg::num_points(multipoint_);
    case ST_MULTILINESTRING: return bg::num_points(multi_linestring_);
    case ST_MULTIPOLYGON: return bg::num_points(multi_polygon_);
    default: return 0;
  }
}

int GeometryWrapperWkb::NumGeometries() const {
  switch (ogc_type_) {
    case ST_POINT:
    case ST_LINESTRING:
    case ST_POLYGON:
      return 1;
    case ST_MULTIPOINT: return multipoint_.size();
    case ST_MULTILINESTRING: return multi_linestring_.size();
    case ST_MULTIPOLYGON: return multi_polygon_.size();
    default: return 0;
  }
}

int GeometryWrapperWkb::NumInteriorRings() const {
  if (ogc_type_ != ST_POLYGON) return -1;
  return bg::interior_rings(polygon_).size();
}

bool GeometryWrapperWkb::IsEmpty() const {
  switch (ogc_type_) {
    case ST_POINT: return bg::num_points(point_) == 0;
    case ST_LINESTRING: return linestring_.empty();
    case ST_POLYGON: return bg::num_points(polygon_) == 0;
    case ST_MULTIPOINT: return multipoint_.empty();
    case ST_MULTILINESTRING: return multi_linestring_.empty();
    case ST_MULTIPOLYGON: return multi_polygon_.empty();
    default: return true;
  }
}

bool GeometryWrapperWkb::IsSimple() const {
  switch (ogc_type_) {
    case ST_POINT: return bg::is_simple(point_);
    case ST_LINESTRING: return bg::is_simple(linestring_);
    case ST_POLYGON: return bg::is_simple(polygon_);
    case ST_MULTIPOINT: return bg::is_simple(multipoint_);
    case ST_MULTILINESTRING: return bg::is_simple(multi_linestring_);
    case ST_MULTIPOLYGON: return bg::is_simple(multi_polygon_);
    default: return false;
  }
}

bool GeometryWrapperWkb::IsClosed() const {
  if (ogc_type_ == ST_LINESTRING) {
    if (linestring_.size() < 2) return false;
    return bg::equals(linestring_.front(), linestring_.back());
  }
  if (ogc_type_ == ST_MULTILINESTRING) {
    for (const linestring2d& ls : multi_linestring_) {
      if (ls.size() < 2) return false;
      if (!bg::equals(ls.front(), ls.back())) return false;
    }
    return true;
  }
  return false;
}

bool GeometryWrapperWkb::IsRing() const {
  if (ogc_type_ != ST_LINESTRING) return false;
  if (linestring_.size() < 2) return false;
  if (!bg::equals(linestring_.front(), linestring_.back())) return false;
  return bg::is_simple(linestring_);
}

bool GeometryWrapperWkb::GetCentroid(double* x, double* y) const {
  point2d centroid;
  switch (ogc_type_) {
    case ST_POINT:
      centroid = point_;
      break;
    case ST_LINESTRING:
      bg::centroid(linestring_, centroid);
      break;
    case ST_POLYGON:
      bg::centroid(polygon_, centroid);
      break;
    case ST_MULTIPOINT:
      bg::centroid(multipoint_, centroid);
      break;
    case ST_MULTILINESTRING:
      bg::centroid(multi_linestring_, centroid);
      break;
    case ST_MULTIPOLYGON:
      bg::centroid(multi_polygon_, centroid);
      break;
    default:
      return false;
  }
  *x = centroid.x();
  *y = centroid.y();
  return true;
}

bool GeometryWrapperWkb::GetStartPoint(double* x, double* y) const {
  if (ogc_type_ != ST_LINESTRING || linestring_.empty()) return false;
  *x = linestring_.front().x();
  *y = linestring_.front().y();
  return true;
}

bool GeometryWrapperWkb::GetEndPoint(double* x, double* y) const {
  if (ogc_type_ != ST_LINESTRING || linestring_.empty()) return false;
  *x = linestring_.back().x();
  *y = linestring_.back().y();
  return true;
}

bool GeometryWrapperWkb::GetPointN(int n, double* x, double* y) const {
  // 1-based indexing; index 0 treated as 1 (matches ESRI behavior).
  int idx = std::max(1, n) - 1;
  if (ogc_type_ == ST_LINESTRING) {
    if (idx < 0 || idx >= static_cast<int>(linestring_.size())) return false;
    *x = linestring_[idx].x();
    *y = linestring_[idx].y();
    return true;
  }
  if (ogc_type_ == ST_MULTIPOINT) {
    if (idx < 0 || idx >= static_cast<int>(multipoint_.size())) return false;
    *x = multipoint_[idx].x();
    *y = multipoint_[idx].y();
    return true;
  }
  return false;
}

bool GeometryWrapperWkb::GetExteriorRing(FunctionContext* ctx,
    StringVal* result) const {
  if (ogc_type_ != ST_POLYGON) return false;
  const auto& ring = bg::exterior_ring(polygon_);
  // Reverse to match ESRI's counter-clockwise exterior ring convention.
  linestring2d ls(ring.rbegin(), ring.rend());
  *result = WriteWkbLineString(ctx, ls);
  return true;
}

bool GeometryWrapperWkb::GetInteriorRingN(FunctionContext* ctx, int n,
    StringVal* result) const {
  if (ogc_type_ != ST_POLYGON) return false;
  const auto& inners = bg::interior_rings(polygon_);
  // 1-based indexing; index 0 treated as 1.
  int idx = std::max(1, n) - 1;
  if (idx < 0 || idx >= static_cast<int>(inners.size())) return false;
  const auto& ring = inners[idx];
  // Reverse to match ESRI's ring ordering convention.
  linestring2d ls(ring.rbegin(), ring.rend());
  *result = WriteWkbLineString(ctx, ls);
  return true;
}

// Helper functions for BufferWrapperWkb.
namespace {

bool parseIntOption(FunctionContext* ctx, const string& value,
    const char* key, std::size_t* out) {
  StringParser::ParseResult parse_result;
  int parsed = StringParser::StringToInt<int>(value.data(), value.size(),
      &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS || parsed <= 0) {
    ctx->SetError(Substitute("Invalid value '$0' for $1", value, key).c_str());
    return false;
  }
  *out = parsed;
  return true;
}

bool parseDoubleOption(FunctionContext* ctx,
    const string& value, const char* key, double* out) {
  StringParser::ParseResult parse_result;
  double parsed = StringParser::StringToFloat<double>(value.data(), value.size(),
      &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS) {
    ctx->SetError(Substitute("Invalid value '$0' for $1", value, key).c_str());
    return false;
  }
  *out = parsed;
  return true;
}

} // Helper functions for BufferWrapperWkb.

bool BufferWrapperWkb::Buffer(FunctionContext* ctx, double distance,
    StringVal* result) const {
  std::variant<point2d, linestring2d, polygon2d, multipoint2d,
      multi_linestring2d, multi_polygon2d> geom;

  switch (ogc_type_) {
    case ST_POINT:
      geom = point_;
      break;
    case ST_LINESTRING:
      geom = linestring_;
      break;
    case ST_POLYGON:
      geom = polygon_;
      break;
    case ST_MULTIPOINT:
      geom = multipoint_;
      break;
    case ST_MULTILINESTRING:
      geom = multi_linestring_;
      break;
    case ST_MULTIPOLYGON:
      geom = multi_polygon_;
      break;
    default:
      return false;
  }

  multi_polygon2d buf;
  auto visit_func = [&buf, this](auto&& geom, auto&& strat_distance, auto&& strat_join,
      auto&& strat_end, auto&& strat_point) {
    bg::buffer(geom, buf, strat_distance, strategy_side_, strat_join, strat_end,
        strat_point);
  };

  if (ctx->IsArgConstant(ARG_DIST_IDX)) {
    std::visit(visit_func, geom, strategy_distance_, strategy_join_, strategy_end_,
        strategy_point_);
  } else {
    std::visit(visit_func, geom, BuildDistanceStrategy(distance), strategy_join_,
        strategy_end_, strategy_point_);
  }

  if (buf.size() == 1) {
    *result = WriteWkbPolygon(ctx, buf.at(0));
  } else {
    *result = WriteWkbMultiPolygon(ctx, buf);
  }

  return true;
}

bool BufferWrapperWkb::ParseBufferStyle(FunctionContext* ctx) {
  using namespace boost::algorithm;
  using namespace strings;
  using namespace strings::delimiter;
  using namespace std;

  StringVal* buffer_style = reinterpret_cast<StringVal*>(
        ctx->GetConstantArg(ARG_BUFFER_STYLE_IDX));

  if (buffer_style->len > BUFFER_STYLE_MAX_LEN) {
    ctx->SetError(Substitute("Buffer style string is too long. Maximum allowed length is "
        "$0.", BUFFER_STYLE_MAX_LEN).c_str());
    return false;
  }

  // Parse the buffer style string into key-value pairs.
  const string style_str(reinterpret_cast<const char*>(buffer_style->ptr),
      buffer_style->len);
  std::unordered_map<string, string> pairs;
  for (StringPiece sp : strings::Split(style_str, AnyOf(" "), SkipEmpty())) {
    pair<string, string> kv = Split(sp, Limit(AnyOf("="), 1), SkipEmpty());
    to_lower(kv.first);

    // Error if unknown or missing keys are present.
    if (UNLIKELY(kv.first != BUFFER_STYLE_KEY_QUAD_SEGS
        && kv.first != BUFFER_STYLE_KEY_ENDCAP && kv.first != BUFFER_STYLE_KEY_MITRE_LIMIT
        && kv.first != BUFFER_STYLE_KEY_SIDE && kv.first != BUFFER_STYLE_KEY_MITER_LIMIT
        && kv.first != BUFFER_STYLE_KEY_JOIN)) {
      ctx->SetError(Substitute("Unknown buffer style key '$0'", kv.first).c_str());
      return false;
    }

    to_lower(kv.second);
    pairs.insert_or_assign(move(kv.first), move(kv.second));
  }

  // Calculate points per circle first since it is used in multiple strategy definitions.
  if (const auto& quad_seg = pairs.find(BUFFER_STYLE_KEY_QUAD_SEGS);
      quad_seg != pairs.cend()) {
    if (parseIntOption(ctx, quad_seg->second, BUFFER_STYLE_KEY_QUAD_SEGS,
        &points_per_circle_)) {
      if (points_per_circle_ > FLAGS_geospatial_max_quad_segs) {
        ctx->SetError(Substitute("Number of quad segments exceeds the maximum allowed "
            "value of $0.", FLAGS_geospatial_max_quad_segs).c_str());
        return false;
      }
      // Multiply the number of segments per quarter circle to get the points per circle.
      points_per_circle_ *= 4;
    } else {
      return false;
    }
  }

  // Point strategy.
  strategy_point_ = DefaultStrategyPoint(points_per_circle_);

  // Endcap style.
  if (const auto& endcap = pairs.find(BUFFER_STYLE_KEY_ENDCAP);
      endcap != pairs.cend()) {
    if (iequals(endcap->second, BUFFER_STYLE_VALUE_SQUARE)) {
      ctx->SetError(Substitute("Endcap value of '$0' is not supported.",
          BUFFER_STYLE_VALUE_SQUARE).c_str());
      return false;
    } else if (iequals(endcap->second, BUFFER_STYLE_VALUE_FLAT)
        || iequals(endcap->second, BUFFER_STYLE_VALUE_BUTT)) {
      strategy_end_ = buff::end_flat();
    } else if (iequals(endcap->second, BUFFER_STYLE_VALUE_ROUND)) {
      strategy_end_ = buff::end_round(points_per_circle_);
    } else {
      ctx->SetError(Substitute("Invalid value '$0' for $1", endcap->second,
          BUFFER_STYLE_KEY_ENDCAP).c_str());
      return false;
    }
  } else {
    strategy_end_ = DefaultStrategyEnd(points_per_circle_);
  }

  // Miter limit.
  double miter_limit = DEFAULT_MITER_LIMIT;
  auto miter = pairs.find(BUFFER_STYLE_KEY_MITRE_LIMIT);
  if (miter == pairs.cend()) miter = pairs.find(BUFFER_STYLE_KEY_MITER_LIMIT);
  if (miter != pairs.cend()
      && !parseDoubleOption(ctx, miter->second, miter->first.c_str(), &miter_limit)) {
    return false;
  }

  // Join style
  if (const auto& join = pairs.find(BUFFER_STYLE_KEY_JOIN); join != pairs.cend()) {
    if (iequals(join->second, BUFFER_STYLE_VALUE_ROUND)) {
      strategy_join_ = buff::join_round(points_per_circle_);
    } else if (iequals(join->second, BUFFER_STYLE_VALUE_MITRE)
        || iequals(join->second, BUFFER_STYLE_VALUE_MITER)) {
      strategy_join_ = buff::join_miter(miter_limit);
    } else if (iequals(join->second, BUFFER_STYLE_VALUE_BEVEL)) {
      ctx->SetError(Substitute("Join value of '$0' is not supported.",
          BUFFER_STYLE_VALUE_BEVEL).c_str());
      return false;
    } else {
      ctx->SetError(Substitute("Invalid value '$0' for $1", join->second,
          BUFFER_STYLE_KEY_JOIN).c_str());
      return false;
    }
  } else {
    strategy_join_ = DefaultStrategyJoin(points_per_circle_);
  }

  // Side style.
  if (const auto& side = pairs.find(BUFFER_STYLE_KEY_SIDE); side != pairs.cend()) {
    if (iequals(side->second, BUFFER_STYLE_VALUE_LEFT)) {
      distance_style_ = DistanceStyle::ASYMMETRIC_LEFT;
    } else if (iequals(side->second, BUFFER_STYLE_VALUE_RIGHT)) {
      distance_style_ = DistanceStyle::ASYMMETRIC_RIGHT;
    } else if (iequals(side->second, BUFFER_STYLE_VALUE_BOTH)) {
      distance_style_ = DistanceStyle::ASYMMETRIC_BOTH;
    } else {
      ctx->SetError(Substitute("Invalid value '$0' for $1", side->second,
          BUFFER_STYLE_KEY_SIDE).c_str());
      return false;
    }
  }

  return true;
}

std::variant<buff::distance_asymmetric<coord_type>, buff::distance_symmetric<coord_type>>
    BufferWrapperWkb::BuildDistanceStrategy(double distance) const {
  switch (distance_style_) {
    case DistanceStyle::ASYMMETRIC_BOTH:
      return buff::distance_asymmetric<coord_type>(distance, distance);
      break;
    case DistanceStyle::ASYMMETRIC_LEFT:
      return buff::distance_asymmetric<coord_type>(distance, 0);
      break;
    case DistanceStyle::ASYMMETRIC_RIGHT:
      return buff::distance_asymmetric<coord_type>(0, distance);
      break;
    default:
      return buff::distance_symmetric<coord_type>(distance);
      break;
  }
}

bool BufferWrapperWkb::InitFromPrepareArgs(FunctionContext* ctx) {
  if (ctx->GetNumArgs() > ARG_BUFFER_STYLE_IDX) {
    if (!ctx->IsArgConstant(ARG_BUFFER_STYLE_IDX)) {
      ctx->SetError("The 'buffer_style' argument must be constant.");
      return false;
    }

    // Parse buffer style args which are space-separated key-value pairs.
    if (!ParseBufferStyle(ctx)) {
      return false;
    }
  }

  if (ctx->GetNumArgs() > ARG_USE_SPHEROID_IDX) {
    if (ctx->IsArgConstant(ARG_USE_SPHEROID_IDX)) {
      BooleanVal* use_spheroid =
          reinterpret_cast<BooleanVal*>(ctx->GetConstantArg(ARG_USE_SPHEROID_IDX));
      if (use_spheroid->val) {
        strategy_point_ = buff::geographic_point_circle<>(points_per_circle_);
      }
    } else {
      ctx->SetError("The 'use_spheroid' argument must be constant.");
      return false;
    }
  }

  if (ctx->IsArgConstant(ARG_DIST_IDX)) {
    DoubleVal* dist = reinterpret_cast<DoubleVal*>(ctx->GetConstantArg(ARG_DIST_IDX));
    strategy_distance_ = BuildDistanceStrategy(dist->val);
  }

  return true;
}

} // namespace impala::geo
