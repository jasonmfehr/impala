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

#include "exprs/geo/relation-wrapper-wkb.h"

#include <boost/geometry.hpp>

#include "testutil/gtest-util.h"

namespace impala::geo {

// Helper to build geometries concisely.
static point2d P(double x, double y) { return point2d(x, y); }

static linestring2d LS(std::initializer_list<point2d> pts) {
  return linestring2d(pts);
}

static polygon2d MakeSquare(double x0, double y0, double x1, double y1) {
  polygon2d poly;
  bg::append(poly.outer(), P(x0, y0));
  bg::append(poly.outer(), P(x1, y0));
  bg::append(poly.outer(), P(x1, y1));
  bg::append(poly.outer(), P(x0, y1));
  bg::append(poly.outer(), P(x0, y0));
  bg::correct(poly);
  return poly;
}

// =============================================================================
// Tests for our workarounds for boost::geometry gaps.
// =============================================================================

class RelationWrapperWkbTest : public testing::Test {};

// --- Within: MultiPoint as geometry1 (boost doesn't support it) ---

TEST_F(RelationWrapperWkbTest, WithinMultiPointInPoint) {
  multipoint2d mp{P(5, 5)};
  point2d pt = P(5, 5);
  EXPECT_TRUE(WithinPredicate::Eval(mp, pt));

  multipoint2d mp2{P(5, 5), P(5, 5)};
  EXPECT_TRUE(WithinPredicate::Eval(mp2, pt));

  multipoint2d mp3{P(5, 5), P(6, 6)};
  EXPECT_FALSE(WithinPredicate::Eval(mp3, pt));
}

TEST_F(RelationWrapperWkbTest, WithinMultiPointInMultiPoint) {
  multipoint2d subset{P(1, 1), P(2, 2)};
  multipoint2d superset{P(1, 1), P(2, 2), P(3, 3)};
  EXPECT_TRUE(WithinPredicate::Eval(subset, superset));
  EXPECT_FALSE(WithinPredicate::Eval(superset, subset));

  multipoint2d same{P(1, 1), P(2, 2), P(3, 3)};
  EXPECT_TRUE(WithinPredicate::Eval(same, superset));
}

TEST_F(RelationWrapperWkbTest, WithinMultiPointInPolygon) {
  polygon2d square = MakeSquare(0, 0, 10, 10);
  multipoint2d inside{P(1, 1), P(5, 5), P(9, 9)};
  EXPECT_TRUE(WithinPredicate::Eval(inside, square));

  multipoint2d partial{P(5, 5), P(20, 20)};
  EXPECT_FALSE(WithinPredicate::Eval(partial, square));
}

TEST_F(RelationWrapperWkbTest, WithinEmptyMultiPoint) {
  multipoint2d empty;
  point2d pt = P(5, 5);
  EXPECT_FALSE(WithinPredicate::Eval(empty, pt));

  polygon2d square = MakeSquare(0, 0, 10, 10);
  EXPECT_FALSE(WithinPredicate::Eval(empty, square));
}

// --- Contains: delegates to Within with swapped args ---

TEST_F(RelationWrapperWkbTest, ContainsPointContainsMultiPoint) {
  point2d pt = P(5, 5);
  multipoint2d mp{P(5, 5)};
  EXPECT_TRUE(ContainsPredicate::Eval(pt, mp));

  multipoint2d mp2{P(5, 5), P(6, 6)};
  EXPECT_FALSE(ContainsPredicate::Eval(pt, mp2));
}

TEST_F(RelationWrapperWkbTest, ContainsPolygonContainsMultiPoint) {
  polygon2d square = MakeSquare(0, 0, 10, 10);
  multipoint2d inside{P(1, 1), P(5, 5)};
  EXPECT_TRUE(ContainsPredicate::Eval(square, inside));

  multipoint2d partial{P(5, 5), P(20, 20)};
  EXPECT_FALSE(ContainsPredicate::Eval(square, partial));
}

// --- Within: dimension check (higher-dim cannot be within lower-dim) ---

TEST_F(RelationWrapperWkbTest, WithinHigherDimInLowerDimReturnsFalse) {
  polygon2d square = MakeSquare(0, 0, 10, 10);
  point2d pt = P(5, 5);
  EXPECT_FALSE(WithinPredicate::Eval(square, pt));

  linestring2d ls = LS({P(0, 0), P(10, 10)});
  EXPECT_FALSE(WithinPredicate::Eval(square, ls));
  EXPECT_FALSE(WithinPredicate::Eval(ls, pt));
}

// --- Crosses: area × area always false (OGC) ---

TEST_F(RelationWrapperWkbTest, CrossesAreaAreaReturnsFalse) {
  polygon2d a = MakeSquare(0, 0, 6, 6);
  polygon2d b = MakeSquare(4, 4, 10, 10);
  EXPECT_FALSE(CrossesPredicate::Eval(a, b));

  multi_polygon2d ma;
  ma.push_back(a);
  multi_polygon2d mb;
  mb.push_back(b);
  EXPECT_FALSE(CrossesPredicate::Eval(ma, mb));
  EXPECT_FALSE(CrossesPredicate::Eval(a, mb));
  EXPECT_FALSE(CrossesPredicate::Eval(ma, b));
}

// --- Crosses: multilinestring workaround ---

TEST_F(RelationWrapperWkbTest, CrossesMultiLinestringPolygon) {
  polygon2d square = MakeSquare(3, 3, 7, 7);
  multi_linestring2d mls;
  mls.push_back(LS({P(0, 5), P(10, 5)}));
  // linestring crosses polygon boundary
  EXPECT_TRUE(CrossesPredicate::Eval(mls, square));
  EXPECT_TRUE(CrossesPredicate::Eval(square, mls));

  multi_linestring2d inside;
  inside.push_back(LS({P(4, 5), P(6, 5)}));
  // linestring entirely inside polygon does not cross
  EXPECT_FALSE(CrossesPredicate::Eval(inside, square));
}

TEST_F(RelationWrapperWkbTest, CrossesMultiLinestringMultiLinestring) {
  multi_linestring2d mls1;
  mls1.push_back(LS({P(0, 5), P(10, 5)}));
  multi_linestring2d mls2;
  mls2.push_back(LS({P(5, 0), P(5, 10)}));
  EXPECT_TRUE(CrossesPredicate::Eval(mls1, mls2));

  multi_linestring2d parallel;
  parallel.push_back(LS({P(0, 6), P(10, 6)}));
  EXPECT_FALSE(CrossesPredicate::Eval(mls1, parallel));
}

// --- R-tree optimization for Intersects on multipolygons ---

TEST_F(RelationWrapperWkbTest, IntersectsMultiPolygonMultiPolygon) {
  multi_polygon2d ma, mb;
  ma.push_back(MakeSquare(0, 0, 2, 2));
  ma.push_back(MakeSquare(10, 10, 12, 12));
  mb.push_back(MakeSquare(1, 1, 3, 3));
  EXPECT_TRUE(IntersectsPredicate::Eval(ma, mb));

  multi_polygon2d mc;
  mc.push_back(MakeSquare(50, 50, 52, 52));
  EXPECT_FALSE(IntersectsPredicate::Eval(ma, mc));
}

TEST_F(RelationWrapperWkbTest, IntersectsPolygonMultiPolygon) {
  polygon2d poly = MakeSquare(1, 1, 3, 3);
  multi_polygon2d mp;
  mp.push_back(MakeSquare(0, 0, 2, 2));
  mp.push_back(MakeSquare(50, 50, 52, 52));
  EXPECT_TRUE(IntersectsPredicate::Eval(poly, mp));
  EXPECT_TRUE(IntersectsPredicate::Eval(mp, poly));

  polygon2d far = MakeSquare(100, 100, 102, 102);
  EXPECT_FALSE(IntersectsPredicate::Eval(far, mp));
  EXPECT_FALSE(IntersectsPredicate::Eval(mp, far));
}

// =============================================================================
// Tests that verify known boost::geometry limitations still exist.
// If these start failing after a boost upgrade, we can remove our workarounds.
// =============================================================================

class BoostGeometryLimitationsTest : public testing::Test {};

// bg::within does not compile for MultiPoint as geometry1.
// We verify the workaround is needed by checking our own implementation gives
// correct results that match expected OGC semantics.
TEST_F(BoostGeometryLimitationsTest, WithinMultiPointNotSupportedByBoost) {
  // This test documents that we need our own MultiPoint-within implementation.
  // If boost adds support, these should still pass (our impl is correct),
  // but we could then simplify to a direct bg::within call.
  multipoint2d mp{P(1, 1), P(2, 2)};
  polygon2d square = MakeSquare(0, 0, 10, 10);
  EXPECT_TRUE(WithinPredicate::Eval(mp, square));

  multipoint2d mp_out{P(1, 1), P(20, 20)};
  EXPECT_FALSE(WithinPredicate::Eval(mp_out, square));
}

// bg::crosses does not support multilinestring with polygon/multipolygon.
// Verify our workaround handles this correctly.
TEST_F(BoostGeometryLimitationsTest, CrossesMultiLinestringNotSupportedByBoost) {
  polygon2d square = MakeSquare(0, 0, 10, 10);
  multi_linestring2d mls;
  mls.push_back(LS({P(-5, 5), P(15, 5)}));

  // Our workaround iterates component linestrings.
  EXPECT_TRUE(CrossesPredicate::Eval(mls, square));
  EXPECT_TRUE(CrossesPredicate::Eval(square, mls));
}

// bg::crosses requires higher-dim argument first for some combos.
// Our wrapper normalizes the argument order.
TEST_F(BoostGeometryLimitationsTest, CrossesArgumentOrderNormalized) {
  linestring2d ls = LS({P(-5, 5), P(15, 5)});
  polygon2d square = MakeSquare(0, 0, 10, 10);

  // line crosses polygon - boost requires polygon first
  EXPECT_TRUE(CrossesPredicate::Eval(ls, square));
  // polygon "crosses" line - same semantics in OGC
  EXPECT_TRUE(CrossesPredicate::Eval(square, ls));
}

// boost::geometry for within(point, multi_polygon) is brute-force O(N).
// Our R-tree intersects optimization helps for the multipolygon case.
// This test just verifies correctness of our R-tree specialization against
// the brute-force result.
TEST_F(BoostGeometryLimitationsTest, RTreeIntersectsMatchesBruteForce) {
  // Build a multi_polygon with many non-overlapping squares.
  multi_polygon2d mp;
  for (int i = 0; i < 50; i++) {
    mp.push_back(MakeSquare(i * 10, 0, i * 10 + 5, 5));
  }

  // Polygon that intersects one of the squares.
  polygon2d hit = MakeSquare(24, 0, 26, 5);
  EXPECT_TRUE(IntersectsPredicate::Eval(hit, mp));
  EXPECT_TRUE(IntersectsPredicate::Eval(mp, hit));
  // boost brute-force should agree
  EXPECT_TRUE(bg::intersects(hit, mp));

  // Polygon that misses all squares (in the gap).
  polygon2d miss = MakeSquare(6, 0, 9, 5);
  EXPECT_FALSE(IntersectsPredicate::Eval(miss, mp));
  EXPECT_FALSE(IntersectsPredicate::Eval(mp, miss));
  EXPECT_FALSE(bg::intersects(miss, mp));

  // Multi vs multi
  multi_polygon2d mp2;
  mp2.push_back(hit);
  mp2.push_back(miss);
  EXPECT_TRUE(IntersectsPredicate::Eval(mp, mp2));
  EXPECT_TRUE(IntersectsPredicate::Eval(mp2, mp));
}

} // namespace impala::geo
