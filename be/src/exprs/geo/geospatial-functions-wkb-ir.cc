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

#include "exprs/geo/geospatial-functions.h"

#include "exprs/geo/boost-common.h"
#include "exprs/geo/geometry-wrapper-wkb.h"
#include "exprs/geo/relation-wrapper-wkb.h"
#include "exprs/geo/bin-utils.h"
#include "exprs/geo/wkb-format.h"
#include "exprs/geo/wkb-serialization.h"
#include "exprs/geo/wkt.h"
#include "runtime/string-value.inline.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "common/names.h"

namespace impala::geo {

static GeometryWrapperWkb* ParseGeom(FunctionContext* ctx, const StringVal& geom) {
  if (geom.is_null) return nullptr;
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(wrapper != nullptr);
  if (!wrapper->FromWkb(geom)) return nullptr;
  return wrapper;
}

// --- Prepare/Close lifecycle ---

void GeospatialFunctions::GeometryWrapperPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  ctx->SetFunctionState(scope, new GeometryWrapperWkb());
}

void GeospatialFunctions::GeometryWrapperBufferPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;

  BufferWrapperWkb* wrapper = new BufferWrapperWkb();
  if (!wrapper->InitFromPrepareArgs(ctx)) {
    delete wrapper;
    return;
  }
  ctx->SetFunctionState(scope, wrapper);
}

void GeospatialFunctions::GeometryWrapperClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  delete reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  ctx->SetFunctionState(scope, nullptr);
}

void GeospatialFunctions::RelationWrapperPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  RelationWrapperWkb* state = new RelationWrapperWkb();
  if (ctx->IsArgConstant(0)) {
    StringVal* lhs = reinterpret_cast<StringVal*>(ctx->GetConstantArg(0));
    if (lhs != nullptr && !lhs->is_null) state->PrepareLhs(ctx, *lhs);
  }
  if (ctx->IsArgConstant(1)) {
    StringVal* rhs = reinterpret_cast<StringVal*>(ctx->GetConstantArg(1));
    if (rhs != nullptr && !rhs->is_null) state->PrepareRhs(ctx, *rhs);
  }
  ctx->SetFunctionState(scope, state);
}

void GeospatialFunctions::RelationWrapperClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  delete reinterpret_cast<RelationWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  ctx->SetFunctionState(scope, nullptr);
}

// --- Accessors ---

DoubleVal GeospatialFunctions::st_X_WKB(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type != ST_POINT) return DoubleVal::null();
  return DoubleVal(GetWkbPointX(geom));
}

DoubleVal GeospatialFunctions::st_Y_WKB(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type != ST_POINT) return DoubleVal::null();
  return DoubleVal(GetWkbPointY(geom));
}

DoubleVal GeospatialFunctions::st_MinX_WKB(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  bool swap;
  if (!ParseWkbHeader(ctx, geom, &ogc_type, &swap)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(GetWkbPointX(geom));
  box2d bbox = ComputeWkbBBox(geom.ptr, geom.len, ogc_type, swap);
  return DoubleVal(bbox.min_corner().x());
}

DoubleVal GeospatialFunctions::st_MinY_WKB(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  bool swap;
  if (!ParseWkbHeader(ctx, geom, &ogc_type, &swap)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(GetWkbPointY(geom));
  box2d bbox = ComputeWkbBBox(geom.ptr, geom.len, ogc_type, swap);
  return DoubleVal(bbox.min_corner().y());
}

DoubleVal GeospatialFunctions::st_MaxX_WKB(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  bool swap;
  if (!ParseWkbHeader(ctx, geom, &ogc_type, &swap)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(GetWkbPointX(geom));
  box2d bbox = ComputeWkbBBox(geom.ptr, geom.len, ogc_type, swap);
  return DoubleVal(bbox.max_corner().x());
}

DoubleVal GeospatialFunctions::st_MaxY_WKB(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  bool swap;
  if (!ParseWkbHeader(ctx, geom, &ogc_type, &swap)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(GetWkbPointY(geom));
  box2d bbox = ComputeWkbBBox(geom.ptr, geom.len, ogc_type, swap);
  return DoubleVal(bbox.max_corner().y());
}

StringVal GeospatialFunctions::st_GeometryType_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return StringVal::null();
  const char* name = OGCTypeToStr[ogc_type];
  return StringVal(name);
}

IntVal GeospatialFunctions::st_Srid_WKB(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return IntVal::null();
  return IntVal(0);
}

StringVal GeospatialFunctions::st_SetSrid_WKB(FunctionContext* ctx,
    const StringVal& geom, const IntVal& srid) {
  if (geom.is_null || srid.is_null) return StringVal::null();
  return StringVal::CopyFrom(ctx, geom.ptr, geom.len);
}

// --- Constructors ---

StringVal GeospatialFunctions::st_Point_WKB(FunctionContext* ctx,
    const DoubleVal& x, const DoubleVal& y) {
  if (x.is_null || y.is_null) return StringVal::null();
  return CreateWkbPoint(ctx, x.val, y.val);
}

StringVal GeospatialFunctions::st_Point_WKB(FunctionContext* ctx,
    const StringVal& wkt) {
  if (wkt.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromWkt(ctx, wkt, ST_POINT)) return StringVal::null();
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_LineString_WKB(FunctionContext* ctx,
    const StringVal& wkt) {
  if (wkt.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromWkt(ctx, wkt, ST_LINESTRING)) return StringVal::null();
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_LineString_WKB(FunctionContext* ctx,
    int num_coords, const DoubleVal* coords) {
  if (num_coords == 0 || num_coords % 2 != 0) {
    ctx->SetError("Invalid number of coordinates");
    return StringVal::null();
  }
  vector<double> raw_coords;
  raw_coords.reserve(num_coords);
  for (int i = 0; i < num_coords; i++) {
    if (coords[i].is_null) {
      ctx->SetError("Null coordinate");
      return StringVal::null();
    }
    raw_coords.push_back(coords[i].val);
  }
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromCoordinates(ctx, ST_LINESTRING, num_coords, raw_coords.data())) {
    return StringVal::null();
  }
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_MultiPoint_WKB(FunctionContext* ctx,
    const StringVal& wkt) {
  if (wkt.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromWkt(ctx, wkt, ST_MULTIPOINT)) return StringVal::null();
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_MultiPoint_WKB(FunctionContext* ctx,
    int num_coords, const DoubleVal* coords) {
  if (num_coords == 0 || num_coords % 2 != 0) {
    ctx->SetError("Invalid number of coordinates");
    return StringVal::null();
  }
  vector<double> raw_coords;
  raw_coords.reserve(num_coords);
  for (int i = 0; i < num_coords; i++) {
    if (coords[i].is_null) {
      ctx->SetError("Null coordinate");
      return StringVal::null();
    }
    raw_coords.push_back(coords[i].val);
  }
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromCoordinates(ctx, ST_MULTIPOINT, num_coords, raw_coords.data())) {
    return StringVal::null();
  }
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_Polygon_WKB(FunctionContext* ctx,
    const StringVal& wkt) {
  if (wkt.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromWkt(ctx, wkt, ST_POLYGON)) return StringVal::null();
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_Polygon_WKB(FunctionContext* ctx,
    int num_coords, const DoubleVal* coords) {
  if (num_coords == 0 || num_coords % 2 != 0) {
    ctx->SetError("Invalid number of coordinates");
    return StringVal::null();
  }
  vector<double> raw_coords;
  raw_coords.reserve(num_coords);
  for (int i = 0; i < num_coords; i++) {
    if (coords[i].is_null) {
      ctx->SetError("Null coordinate");
      return StringVal::null();
    }
    raw_coords.push_back(coords[i].val);
  }
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromCoordinates(ctx, ST_POLYGON, num_coords, raw_coords.data())) {
    return StringVal::null();
  }
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_MultiLineString_WKB(FunctionContext* ctx,
    const StringVal& wkt) {
  if (wkt.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromWkt(ctx, wkt, ST_MULTILINESTRING)) return StringVal::null();
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_MultiPolygon_WKB(FunctionContext* ctx,
    const StringVal& wkt) {
  if (wkt.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromWkt(ctx, wkt, ST_MULTIPOLYGON)) return StringVal::null();
  return wrapper->ToWkb(ctx);
}

// --- Predicates ---

BooleanVal GeospatialFunctions::st_EnvIntersects_WKB(FunctionContext* ctx,
    const StringVal& lhs, const StringVal& rhs) {
  OGCType lhs_type, rhs_type;
  bool lhs_swap, rhs_swap;
  if (!ParseWkbHeader(ctx, lhs, &lhs_type, &lhs_swap) ||
      !ParseWkbHeader(ctx, rhs, &rhs_type, &rhs_swap)) {
    return BooleanVal::null();
  }
  box2d lhs_box = ComputeWkbBBox(lhs.ptr, lhs.len, lhs_type, lhs_swap);
  box2d rhs_box = ComputeWkbBBox(rhs.ptr, rhs.len, rhs_type, rhs_swap);
  bool intersects = !(lhs_box.max_corner().x() < rhs_box.min_corner().x() ||
      rhs_box.max_corner().x() < lhs_box.min_corner().x() ||
      lhs_box.max_corner().y() < rhs_box.min_corner().y() ||
      rhs_box.max_corner().y() < lhs_box.min_corner().y());
  return BooleanVal(intersects);
}

#define DEFINE_RELATION_WKB(relation_name)                                       \
BooleanVal GeospatialFunctions::st_##relation_name##_WKB(                        \
    FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs) {          \
  return RelationWrapperWkb::EvalWkbWkb<relation_name##Predicate>(ctx, lhs, rhs);\
}

DEFINE_RELATION_WKB(Contains)
DEFINE_RELATION_WKB(Crosses)
DEFINE_RELATION_WKB(Disjoint)
DEFINE_RELATION_WKB(Equals)
DEFINE_RELATION_WKB(Intersects)
DEFINE_RELATION_WKB(Overlaps)
DEFINE_RELATION_WKB(Touches)
DEFINE_RELATION_WKB(Within)

#undef DEFINE_RELATION_WKB

// --- Transformations ---

StringVal GeospatialFunctions::st_Envelope_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  OGCType ogc_type;
  bool swap;
  if (!ParseWkbHeader(ctx, geom, &ogc_type, &swap)) return StringVal::null();
  box2d bbox = ComputeWkbBBox(geom.ptr, geom.len, ogc_type, swap);
  return WriteWkbBox(ctx, bbox);
}

StringVal GeospatialFunctions::st_AsText_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  return wrapper->ToWkt(ctx);
}

StringVal GeospatialFunctions::st_GeomFromText_WKB(FunctionContext* ctx,
    const StringVal& wkt) {
  if (wkt.is_null) return StringVal::null();
  OGCType type = GetTypeFromWkt(wkt);
  if (type == UNKNOWN) {
    ctx->SetError("Unable to determine geometry type from WKT");
    return StringVal::null();
  }
  GeometryWrapperWkb* wrapper = reinterpret_cast<GeometryWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!wrapper->FromWkt(ctx, wkt, type)) return StringVal::null();
  return wrapper->ToWkb(ctx);
}

StringVal GeospatialFunctions::st_GeomFromText_WKB(FunctionContext* ctx,
    const StringVal& wkt, const IntVal& srid) {
  // SRID is ignored in WKB mode.
  return st_GeomFromText_WKB(ctx, wkt);
}

// --- Binning ---

BigIntVal GeospatialFunctions::st_BinGeom_WKB(FunctionContext* ctx,
    const BigIntVal& bin_size, const StringVal& geom) {
  if (bin_size.is_null || geom.is_null) return BigIntVal::null();
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return BigIntVal::null();
  if (ogc_type != ST_POINT) return BigIntVal::null();
  double x = GetWkbPointX(geom);
  double y = GetWkbPointY(geom);
  return BigIntVal(getBinId(bin_size.val, x, y));
}

BigIntVal GeospatialFunctions::st_BinGeom_WKB(FunctionContext* ctx,
    const DoubleVal& bin_size, const StringVal& geom) {
  if (bin_size.is_null || geom.is_null) return BigIntVal::null();
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return BigIntVal::null();
  if (ogc_type != ST_POINT) return BigIntVal::null();
  double x = GetWkbPointX(geom);
  double y = GetWkbPointY(geom);
  return BigIntVal(getBinId(bin_size.val, x, y));
}

BigIntVal GeospatialFunctions::st_BinWkt(FunctionContext* ctx,
    const BigIntVal& bin_size, const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return BigIntVal::null();
  point2d point;
  if (!wktToPoint(ctx, wkt, point)) return BigIntVal::null();
  return BigIntVal(getBinId(bin_size.val, point.x(), point.y()));
}

BigIntVal GeospatialFunctions::st_BinWkt(FunctionContext* ctx,
    const DoubleVal& bin_size, const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return BigIntVal::null();
  point2d point;
  if (!wktToPoint(ctx, wkt, point)) return BigIntVal::null();
  return BigIntVal(getBinId(bin_size.val, point.x(), point.y()));
}

StringVal GeospatialFunctions::st_BinenvelopeBinId_WKB(FunctionContext* ctx,
    const BigIntVal& bin_size, const BigIntVal& bin_id) {
  if (bin_size.is_null || bin_id.is_null) return StringVal::null();
  box2d envelope = getBinEnvelope(bin_size.val, bin_id.val);
  return WriteWkbBox(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeBinId_WKB(FunctionContext* ctx,
    const DoubleVal& bin_size, const BigIntVal& bin_id) {
  if (bin_size.is_null || bin_id.is_null) return StringVal::null();
  box2d envelope = getBinEnvelope(bin_size.val, bin_id.val);
  return WriteWkbBox(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeGeom_WKB(FunctionContext* ctx,
    const BigIntVal& bin_size, const StringVal& geom) {
  if (bin_size.is_null || geom.is_null) return StringVal::null();
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return StringVal::null();
  if (ogc_type != ST_POINT) {
    ctx->SetError("st_BinEnvelope with geometry only supports POINT");
    return StringVal::null();
  }
  double x = GetWkbPointX(geom);
  double y = GetWkbPointY(geom);
  box2d envelope = getBinEnvelope(bin_size.val, x, y);
  return WriteWkbBox(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeGeom_WKB(FunctionContext* ctx,
    const DoubleVal& bin_size, const StringVal& geom) {
  if (bin_size.is_null || geom.is_null) return StringVal::null();
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return StringVal::null();
  if (ogc_type != ST_POINT) {
    ctx->SetError("st_BinEnvelope with geometry only supports POINT");
    return StringVal::null();
  }
  double x = GetWkbPointX(geom);
  double y = GetWkbPointY(geom);
  box2d envelope = getBinEnvelope(bin_size.val, x, y);
  return WriteWkbBox(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeWkt_WKB(FunctionContext* ctx,
    const BigIntVal& bin_size, const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return StringVal::null();
  point2d point;
  if (!wktToPoint(ctx, wkt, point)) return StringVal::null();
  box2d envelope = getBinEnvelope(bin_size.val, point.x(), point.y());
  return WriteWkbBox(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeWkt_WKB(FunctionContext* ctx,
    const DoubleVal& bin_size, const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return StringVal::null();
  point2d point;
  if (!wktToPoint(ctx, wkt, point)) return StringVal::null();
  box2d envelope = getBinEnvelope(bin_size.val, point.x(), point.y());
  return WriteWkbBox(ctx, envelope);
}

// --- Geometry property functions ---

DoubleVal GeospatialFunctions::st_Area_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return DoubleVal::null();
  return DoubleVal(wrapper->Area());
}

DoubleVal GeospatialFunctions::st_Length_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return DoubleVal::null();
  return DoubleVal(wrapper->Length());
}

DoubleVal GeospatialFunctions::st_Distance_WKB(FunctionContext* ctx,
    const StringVal& lhs_geom, const StringVal& rhs_geom) {
  RelationWrapperWkb* rel = reinterpret_cast<RelationWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  if (!rel->lhs_prepared()) {
    if (!rel->lhs().FromWkb(lhs_geom)) return DoubleVal::null();
  }
  if (!rel->rhs_prepared()) {
    if (!rel->rhs().FromWkb(rhs_geom)) return DoubleVal::null();
  }
  double d = rel->lhs().Distance(rel->rhs());
  if (d < 0) return DoubleVal::null();
  return DoubleVal(d);
}

IntVal GeospatialFunctions::st_Dimension_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseWkbHeader(ctx, geom, &ogc_type)) return IntVal::null();
  switch (ogc_type) {
    case ST_POINT:
    case ST_MULTIPOINT:
      return IntVal(0);
    case ST_LINESTRING:
    case ST_MULTILINESTRING:
      return IntVal(1);
    case ST_POLYGON:
    case ST_MULTIPOLYGON:
      return IntVal(2);
    default:
      return IntVal::null();
  }
}

// TODO: could parse the point count directly from WKB bytes, avoiding full deserialization.
IntVal GeospatialFunctions::st_NumPoints_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return IntVal::null();
  return IntVal(wrapper->NumPoints());
}

// TODO: could read the sub-geometry count directly from WKB bytes (offset 5-8 for multi types).
IntVal GeospatialFunctions::st_NumGeometries_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return IntVal::null();
  return IntVal(wrapper->NumGeometries());
}

// TODO: could read the ring count directly from WKB bytes (offset 5-8 for polygon).
IntVal GeospatialFunctions::st_NumInteriorRing_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return IntVal::null();
  int n = wrapper->NumInteriorRings();
  if (n < 0) return IntVal::null();
  return IntVal(n);
}

// TODO: could check emptiness directly from WKB bytes (count field == 0 for multi types).
BooleanVal GeospatialFunctions::st_IsEmpty_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return BooleanVal::null();
  return BooleanVal(wrapper->IsEmpty());
}

BooleanVal GeospatialFunctions::st_IsSimple_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return BooleanVal::null();
  return BooleanVal(wrapper->IsSimple());
}

BooleanVal GeospatialFunctions::st_IsClosed_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return BooleanVal::null();
  OGCType t = wrapper->type();
  if (t != ST_LINESTRING && t != ST_MULTILINESTRING) return BooleanVal::null();
  return BooleanVal(wrapper->IsClosed());
}

BooleanVal GeospatialFunctions::st_IsRing_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return BooleanVal::null();
  if (wrapper->type() != ST_LINESTRING) return BooleanVal::null();
  return BooleanVal(wrapper->IsRing());
}

// --- Geometry-returning property functions ---

StringVal GeospatialFunctions::st_Centroid_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  double x, y;
  if (!wrapper->GetCentroid(&x, &y)) return StringVal::null();
  return CreateWkbPoint(ctx, x, y);
}

StringVal GeospatialFunctions::st_StartPoint_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  double x, y;
  if (!wrapper->GetStartPoint(&x, &y)) return StringVal::null();
  return CreateWkbPoint(ctx, x, y);
}

StringVal GeospatialFunctions::st_EndPoint_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  double x, y;
  if (!wrapper->GetEndPoint(&x, &y)) return StringVal::null();
  return CreateWkbPoint(ctx, x, y);
}

StringVal GeospatialFunctions::st_PointN_WKB(FunctionContext* ctx,
    const StringVal& geom, const IntVal& n) {
  if (n.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  double x, y;
  if (!wrapper->GetPointN(n.val, &x, &y)) return StringVal::null();
  return CreateWkbPoint(ctx, x, y);
}

StringVal GeospatialFunctions::st_ExteriorRing_WKB(FunctionContext* ctx,
    const StringVal& geom) {
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  StringVal result;
  if (!wrapper->GetExteriorRing(ctx, &result)) return StringVal::null();
  return result;
}

StringVal GeospatialFunctions::st_InteriorRingN_WKB(FunctionContext* ctx,
    const StringVal& geom, const IntVal& n) {
  if (n.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  StringVal result;
  if (!wrapper->GetInteriorRingN(ctx, n.val, &result)) return StringVal::null();
  return result;
}

StringVal GeospatialFunctions::st_Buffer_WKB(FunctionContext* ctx, const StringVal& geom,
    const DoubleVal& distance) {
  if (geom.is_null || distance.is_null) return StringVal::null();
  GeometryWrapperWkb* wrapper = ParseGeom(ctx, geom);
  if (!wrapper) return StringVal::null();
  BufferWrapperWkb* buffer_wrapper = static_cast<BufferWrapperWkb*>(wrapper);

  StringVal result;
  if (!buffer_wrapper->Buffer(ctx, distance.val, &result)) return StringVal::null();
  return result;
}

StringVal GeospatialFunctions::st_Buffer_WKB(FunctionContext* ctx, const StringVal& geom,
    const DoubleVal& distance, const BooleanVal& use_spheroid) {
  if (use_spheroid.is_null) return StringVal::null();
  return st_Buffer_WKB(ctx, geom, distance);
}

StringVal GeospatialFunctions::st_Buffer_WKB(FunctionContext* ctx, const StringVal& geom,
    const DoubleVal& distance, const BooleanVal& use_spheroid,
    const StringVal& buffer_style) {
  if (use_spheroid.is_null || buffer_style.is_null) return StringVal::null();
  return st_Buffer_WKB(ctx, geom, distance);
}

} // namespace impala::geo
