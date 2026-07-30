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

#pragma once

#include <map>

#include "common/status.h"
#include "udf/udf.h"

namespace impala::geo {

using impala_udf::FunctionContext;
using impala_udf::BooleanVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::DoubleVal;
using impala_udf::StringVal;

class Expr;
class OpcodeRegistry;
struct StringValue;
class TupleRow;

class GeospatialFunctions {
 public:
  // --- Prepare/Close lifecycle for WKB functions ---
  static void GeometryWrapperPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void GeometryWrapperBufferPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void GeometryWrapperClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void RelationWrapperPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void RelationWrapperClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  // --- ESRI Shape format functions (HIVE_ESRI mode) ---

  // Accessors
  static DoubleVal st_X(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_Y(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinX(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinY(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxX(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxY(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_GeometryType(FunctionContext* ctx, const StringVal& geom);
  static IntVal st_Srid(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_SetSrid(FunctionContext* ctx, const StringVal& geom,
      const IntVal& srid);

  // Constructors
  static StringVal st_Point(FunctionContext* ctx, const DoubleVal& x, const DoubleVal& y);

  // Predicates
  static BooleanVal st_EnvIntersects(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);

  // --- WKB format functions (WKB_EXPERIMENTAL mode) ---

  // Accessors
  static DoubleVal st_X_WKB(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_Y_WKB(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinX_WKB(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinY_WKB(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxX_WKB(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxY_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_GeometryType_WKB(FunctionContext* ctx, const StringVal& geom);
  static IntVal st_Srid_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_SetSrid_WKB(FunctionContext* ctx, const StringVal& geom,
      const IntVal& srid);

  // Constructors
  static StringVal st_Point_WKB(FunctionContext* ctx,
      const DoubleVal& x, const DoubleVal& y);
  static StringVal st_Point_WKB(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_LineString_WKB(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_LineString_WKB(FunctionContext* ctx, int num_coords,
      const DoubleVal* coords);
  static StringVal st_MultiPoint_WKB(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_MultiPoint_WKB(FunctionContext* ctx, int num_coords,
      const DoubleVal* coords);
  static StringVal st_Polygon_WKB(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_Polygon_WKB(FunctionContext* ctx, int num_coords,
      const DoubleVal* coords);
  static StringVal st_MultiLineString_WKB(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_MultiPolygon_WKB(FunctionContext* ctx, const StringVal& wkt);

  // Predicates
  static BooleanVal st_EnvIntersects_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);

  static BooleanVal st_Contains_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);
  static BooleanVal st_Crosses_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);
  static BooleanVal st_Disjoint_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);
  static BooleanVal st_Equals_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);
  static BooleanVal st_Intersects_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);
  static BooleanVal st_Overlaps_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);
  static BooleanVal st_Touches_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);
  static BooleanVal st_Within_WKB(
      FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs);

  // Geometry property functions
  static DoubleVal st_Area_WKB(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_Length_WKB(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_Distance_WKB(FunctionContext* ctx,
      const StringVal& lhs, const StringVal& rhs);
  static IntVal st_Dimension_WKB(FunctionContext* ctx, const StringVal& geom);
  static IntVal st_NumPoints_WKB(FunctionContext* ctx, const StringVal& geom);
  static IntVal st_NumGeometries_WKB(FunctionContext* ctx, const StringVal& geom);
  static IntVal st_NumInteriorRing_WKB(FunctionContext* ctx, const StringVal& geom);
  static BooleanVal st_IsEmpty_WKB(FunctionContext* ctx, const StringVal& geom);
  static BooleanVal st_IsSimple_WKB(FunctionContext* ctx, const StringVal& geom);
  static BooleanVal st_IsClosed_WKB(FunctionContext* ctx, const StringVal& geom);
  static BooleanVal st_IsRing_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_Centroid_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_StartPoint_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_EndPoint_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_PointN_WKB(FunctionContext* ctx, const StringVal& geom,
      const IntVal& n);
  static StringVal st_ExteriorRing_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_InteriorRingN_WKB(FunctionContext* ctx, const StringVal& geom,
      const IntVal& n);
  static StringVal st_Buffer_WKB(FunctionContext* ctx, const StringVal& geom,
      const DoubleVal& distance);
  static StringVal st_Buffer_WKB(FunctionContext* ctx, const StringVal& geom,
      const DoubleVal& distance, const BooleanVal& use_spheroid);
  static StringVal st_Buffer_WKB(FunctionContext* ctx, const StringVal& geom,
      const DoubleVal& distance, const BooleanVal& use_spheroid,
      const StringVal& buffer_style);

  // Transformations
  static StringVal st_Envelope_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_AsText_WKB(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_GeomFromText_WKB(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_GeomFromText_WKB(FunctionContext* ctx, const StringVal& wkt,
      const IntVal& srid);

  // Binning
  static BigIntVal st_BinGeom_WKB(FunctionContext* ctx, const BigIntVal& bin_size,
      const StringVal& geom);
  static BigIntVal st_BinGeom_WKB(FunctionContext* ctx, const DoubleVal& bin_size,
      const StringVal& geom);
  static BigIntVal st_BinWkt(FunctionContext* ctx, const BigIntVal& bin_size,
      const StringVal& wkt);
  static BigIntVal st_BinWkt(FunctionContext* ctx, const DoubleVal& bin_size,
      const StringVal& wkt);
  static StringVal st_BinenvelopeBinId_WKB(FunctionContext* ctx,
      const BigIntVal& bin_size, const BigIntVal& bin_id);
  static StringVal st_BinenvelopeBinId_WKB(FunctionContext* ctx,
      const DoubleVal& bin_size, const BigIntVal& bin_id);
  static StringVal st_BinenvelopeGeom_WKB(FunctionContext* ctx,
      const BigIntVal& bin_size, const StringVal& geom);
  static StringVal st_BinenvelopeGeom_WKB(FunctionContext* ctx,
      const DoubleVal& bin_size, const StringVal& geom);
  static StringVal st_BinenvelopeWkt_WKB(FunctionContext* ctx,
      const BigIntVal& bin_size, const StringVal& wkt);
  static StringVal st_BinenvelopeWkt_WKB(FunctionContext* ctx,
      const DoubleVal& bin_size, const StringVal& wkt);
};

} // namespace impala::geo
