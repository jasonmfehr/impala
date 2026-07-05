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

#include <string>

#include "exprs/geo/boost-common.h"
#include "exprs/geo/wkb-format.h"

namespace impala::geo {

// Wraps a single geometry instance, providing deserialization from WKB/WKT and
// serialization back to WKB/WKT. Holds one of each geometry type as members;
// only the one matching ogc_type_ is populated.
class GeometryWrapperWkb {
 public:
  bool FromWkb(const StringVal& geom);
  bool FromWkt(FunctionContext* ctx, const StringVal& wkt, OGCType type);
  bool FromCoordinates(FunctionContext* ctx, OGCType type, int num_coords,
      const double* coords);

  StringVal ToWkb(FunctionContext* ctx) const;
  StringVal ToWkt(FunctionContext* ctx);

  OGCType type() const { return ogc_type_; }
  const point2d& point() const { return point_; }
  const linestring2d& linestring() const { return linestring_; }
  const polygon2d& polygon() const { return polygon_; }
  const multipoint2d& multipoint() const { return multipoint_; }
  const multi_linestring2d& multi_linestring() const { return multi_linestring_; }
  const multi_polygon2d& multi_polygon() const { return multi_polygon_; }

  // Geometry property methods (boost::geometry calls live here, not in IR code).
  double Area() const;
  double Length() const;
  double Distance(const GeometryWrapperWkb& other) const;
  int NumPoints() const;
  int NumGeometries() const;
  int NumInteriorRings() const;
  bool IsEmpty() const;
  bool IsSimple() const;
  bool IsClosed() const;
  bool IsRing() const;
  bool GetCentroid(double* x, double* y) const;
  bool GetStartPoint(double* x, double* y) const;
  bool GetEndPoint(double* x, double* y) const;
  bool GetPointN(int n, double* x, double* y) const;
  bool GetExteriorRing(FunctionContext* ctx, StringVal* result) const;
  bool GetInteriorRingN(FunctionContext* ctx, int n, StringVal* result) const;

 private:
  void appendDouble(double val);
  void appendCoord(double x, double y);
  void appendRing(const bg::model::ring<point2d, true>& ring, bool reverse);
  void appendPolygonBody(const polygon2d& poly);

  OGCType ogc_type_ = UNKNOWN;
  point2d point_;
  linestring2d linestring_;
  polygon2d polygon_;
  multipoint2d multipoint_;
  multi_linestring2d multi_linestring_;
  multi_polygon2d multi_polygon_;
  std::string wkt_buf_;
};

} // namespace impala::geo
