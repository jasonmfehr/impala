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
#include <cstring>

#include <gutil/strings/numbers.h>

#include "exprs/anyval-util.h"
#include "exprs/geo/wkb-format.h"
#include "exprs/geo/wkb-serialization.h"
#include "exprs/geo/wkt.h"
#include "udf/udf.h"

#include "common/names.h"

namespace impala::geo {

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

} // namespace impala::geo
