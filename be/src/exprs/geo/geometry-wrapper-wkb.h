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
#include <variant>

#include <gtest/gtest_prod.h>

#include "exprs/geo/boost-common.h"
#include "exprs/geo/wkb-format.h"

namespace impala::geo {

namespace buff = bg::strategy::buffer;

// Wraps a single geometry instance, providing deserialization from WKB/WKT and
// serialization back to WKB/WKT. Holds one of each geometry type as members;
// only the one matching ogc_type_ is populated.
class GeometryWrapperWkb {
 public:
  // Enable GeospatialFunctions::GeometryWrapperClose() to properly delete subclasses.
  virtual ~GeometryWrapperWkb() = default;

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

 protected:
  OGCType ogc_type_ = UNKNOWN;
  point2d point_;
  linestring2d linestring_;
  polygon2d polygon_;
  multipoint2d multipoint_;
  multi_linestring2d multi_linestring_;
  multi_polygon2d multi_polygon_;
  std::string wkt_buf_;

 private:
  void appendDouble(double val);
  void appendCoord(double x, double y);
  void appendRing(const bg::model::ring<point2d, true>& ring, bool reverse);
  void appendPolygonBody(const polygon2d& poly);
};

// Wraps ST_Buffer-specific setup/validation and execution helpers.
class BufferWrapperWkb : public GeometryWrapperWkb {
 public:
  // Initializes class member variables from the function arguments. Does not check if any
  // argument is NULL, those checks must be done before calling this function.
  bool InitFromPrepareArgs(FunctionContext* ctx);
  bool Buffer(FunctionContext* ctx, double distance, StringVal* result) const;

 private:
  FRIEND_TEST(BufferWrapperWkbTest, DefaultStrategies);
  FRIEND_TEST(BufferWrapperWkbTest, DistanceSpecified);

  enum class DistanceStyle {
    SYMMETRIC,
    ASYMMETRIC_BOTH,
    ASYMMETRIC_LEFT,
    ASYMMETRIC_RIGHT
  };

  bool ParseBufferStyle(FunctionContext* ctx);
  std::variant<buff::distance_asymmetric<coord_type>,
      buff::distance_symmetric<coord_type>> BuildDistanceStrategy(double distance) const;

  static buff::join_round DefaultStrategyJoin(std::size_t points_per_circle) {
    return buff::join_round(points_per_circle);
  }
  static buff::end_round DefaultStrategyEnd(std::size_t points_per_circle) {
    return buff::end_round(points_per_circle);
  }
  static buff::point_circle DefaultStrategyPoint(std::size_t points_per_circle) {
    return buff::point_circle(points_per_circle);
  }

  // Buffer strategy configuration set at prepare-time.
  std::variant<buff::distance_asymmetric<coord_type>,
      buff::distance_symmetric<coord_type>> strategy_distance_ =
      buff::distance_symmetric<coord_type>(1);
  const buff::side_straight strategy_side_ = buff::side_straight();
  std::variant<buff::join_round, buff::join_miter> strategy_join_ =
      DefaultStrategyJoin(DEFAULT_POINTS_PER_CIRCLE);
  std::variant<buff::end_round, buff::end_flat> strategy_end_ =
      DefaultStrategyEnd(DEFAULT_POINTS_PER_CIRCLE);
  std::variant<buff::point_circle, buff::point_square, buff::geographic_point_circle<>>
      strategy_point_ = DefaultStrategyPoint(DEFAULT_POINTS_PER_CIRCLE);
  DistanceStyle distance_style_ = DistanceStyle::SYMMETRIC;
  std::size_t points_per_circle_ = DEFAULT_POINTS_PER_CIRCLE;
};

} // namespace impala::geo
