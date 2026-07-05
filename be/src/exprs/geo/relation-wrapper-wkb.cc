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

#include <type_traits>

#include <boost/geometry/index/rtree.hpp>

#include "exprs/geo/wkb-format.h"

#include "common/names.h"

namespace impala::geo {

// --- Prepare support ---

bool RelationWrapperWkb::PrepareLhs(FunctionContext* ctx, const StringVal& geom) {
  if (!ParseWkbHeader(ctx, geom, &lhs_type_)) return false;
  if (!lhs_.FromWkb(geom)) return false;
  lhs_prepared_ = true;
  return true;
}

bool RelationWrapperWkb::PrepareRhs(FunctionContext* ctx, const StringVal& geom) {
  if (!ParseWkbHeader(ctx, geom, &rhs_type_)) return false;
  if (!rhs_.FromWkb(geom)) return false;
  rhs_prepared_ = true;
  return true;
}

// --- Entry point: both operands are WKB ---

template <class TPredicate>
BooleanVal RelationWrapperWkb::EvalWkbWkb(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom) {
  RelationWrapperWkb* rel = reinterpret_cast<RelationWrapperWkb*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));

  if (!rel->lhs_prepared_) {
    if (!ParseWkbHeader(ctx, lhs_geom, &rel->lhs_type_)) return BooleanVal::null();
    if (!rel->lhs_.FromWkb(lhs_geom)) return BooleanVal::null();
  }
  if (!rel->rhs_prepared_) {
    if (!ParseWkbHeader(ctx, rhs_geom, &rel->rhs_type_)) return BooleanVal::null();
    if (!rel->rhs_.FromWkb(rhs_geom)) return BooleanVal::null();
  }

  return rel->Eval<TPredicate>(ctx);
}

// --- Predicate implementations ---

template <class lhs_geometry_t, class rhs_geometry_t>
bool DisjointPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return bg::disjoint(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool EqualsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return bg::equals(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool IntersectsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return bg::intersects(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool OverlapsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return bg::overlaps(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool TouchesPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return bg::touches(lhs, rhs);
}

// --- Within / Contains ---

template <class lhs_geometry_t, class rhs_geometry_t>
bool WithinPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  constexpr int lhs_dim = bg::topological_dimension<lhs_geometry_t>();
  constexpr int rhs_dim = bg::topological_dimension<rhs_geometry_t>();
  if constexpr (lhs_dim > rhs_dim) {
    return false;
  } else if constexpr (std::is_same_v<lhs_geometry_t, multipoint2d>) {
    // bg::within does not support MultiPoint as geometry1.
    if (lhs.empty()) return false;
    if constexpr (std::is_same_v<rhs_geometry_t, point2d>) {
      for (const auto& pt : lhs) {
        if (!bg::equals(pt, rhs)) return false;
      }
    } else if constexpr (std::is_same_v<rhs_geometry_t, multipoint2d>) {
      for (const auto& pt : lhs) {
        bool found = false;
        for (const auto& rpt : rhs) {
          if (bg::equals(pt, rpt)) { found = true; break; }
        }
        if (!found) return false;
      }
    } else {
      for (const auto& pt : lhs) {
        if (!bg::within(pt, rhs)) return false;
      }
    }
    return true;
  } else {
    return bg::within(lhs, rhs);
  }
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool ContainsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return WithinPredicate::Eval(rhs, lhs);
}

// --- Crosses ---

template <class lhs_geometry_t, class rhs_geometry_t>
bool CrossesPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  constexpr int lhs_dim = bg::topological_dimension<lhs_geometry_t>();
  constexpr int rhs_dim = bg::topological_dimension<rhs_geometry_t>();
  if constexpr (lhs_dim == 2 && rhs_dim == 2) {
    // OGC: crosses is undefined for area × area.
    return false;
  } else if constexpr (std::is_same_v<lhs_geometry_t, multi_linestring2d>
      || std::is_same_v<rhs_geometry_t, multi_linestring2d>) {
    // boost doesn't support polygon/multipolygon × multilinestring.
    // Iterate component linestrings against the other geometry.
    if constexpr (std::is_same_v<lhs_geometry_t, multi_linestring2d>) {
      for (const auto& ls : lhs) {
        if (bg::crosses(rhs, ls)) return true;
      }
    } else {
      for (const auto& ls : rhs) {
        if (bg::crosses(lhs, ls)) return true;
      }
    }
    return false;
  } else if constexpr (lhs_dim < rhs_dim) {
    // boost only supports higher-dim first for some combinations.
    return bg::crosses(rhs, lhs);
  } else {
    return bg::crosses(lhs, rhs);
  }
}

// --- R-tree optimization for IntersectsPredicate on multipolygons ---

typedef std::pair<box2d, uint32_t> rtree_val_t;
using rtree_t = bg::index::rtree<
    rtree_val_t, bg::index::quadratic<16>>;

static void buildRTree(const multi_polygon2d& mpoly, rtree_t* rtree) {
  for (uint32_t i = 0; i < mpoly.size(); i++) {
    box2d mbr;
    bg::envelope(mpoly[i], mbr);
    rtree->insert(rtree_val_t(mbr, i));
  }
}

static bool treeAssistedIntersect(
    const multi_polygon2d& mpoly, const polygon2d& poly, const rtree_t& rtree) {
  box2d mbr;
  bg::envelope(poly, mbr);
  auto it = rtree.qbegin(bg::index::intersects(mbr));
  for (; it != rtree.qend(); it++) {
    if (bg::intersects(poly, mpoly[it->second])) return true;
  }
  return false;
}

template <>
bool IntersectsPredicate::Eval(
    const multi_polygon2d& lhs, const multi_polygon2d& rhs) {
  const multi_polygon2d& smaller = lhs.size() < rhs.size() ? lhs : rhs;
  const multi_polygon2d& bigger = lhs.size() < rhs.size() ? rhs : lhs;
  rtree_t rtree;
  buildRTree(smaller, &rtree);
  for (const polygon2d& poly : bigger) {
    if (treeAssistedIntersect(smaller, poly, rtree)) return true;
  }
  return false;
}

template <>
bool IntersectsPredicate::Eval(const polygon2d& lhs, const multi_polygon2d& rhs) {
  rtree_t rtree;
  buildRTree(rhs, &rtree);
  return treeAssistedIntersect(rhs, lhs, rtree);
}

template <>
bool IntersectsPredicate::Eval(const multi_polygon2d& lhs, const polygon2d& rhs) {
  rtree_t rtree;
  buildRTree(lhs, &rtree);
  return treeAssistedIntersect(lhs, rhs, rtree);
}

// --- Template dispatch chain ---

template <class TPredicate, class lhs_geometry_t, class rhs_geometry_t>
BooleanVal RelationWrapperWkb::EvalInner2(FunctionContext* ctx,
    const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return BooleanVal(TPredicate::Eval(lhs, rhs));
}

template <class TPredicate, class lhs_geometry_t>
BooleanVal RelationWrapperWkb::EvalInner(FunctionContext* ctx,
    const lhs_geometry_t& lhs) {
  switch (rhs_type_) {
    case ST_POINT:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.point());
    case ST_LINESTRING:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.linestring());
    case ST_POLYGON:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.polygon());
    case ST_MULTIPOINT:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.multipoint());
    case ST_MULTILINESTRING:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.multi_linestring());
    case ST_MULTIPOLYGON:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.multi_polygon());
    default:
      return BooleanVal::null();
  }
}

template <class TPredicate>
BooleanVal RelationWrapperWkb::Eval(FunctionContext* ctx) {
  try {
    switch (lhs_type_) {
      case ST_POINT:
        return EvalInner<TPredicate>(ctx, lhs_.point());
      case ST_LINESTRING:
        return EvalInner<TPredicate>(ctx, lhs_.linestring());
      case ST_POLYGON:
        return EvalInner<TPredicate>(ctx, lhs_.polygon());
      case ST_MULTIPOINT:
        return EvalInner<TPredicate>(ctx, lhs_.multipoint());
      case ST_MULTILINESTRING:
        return EvalInner<TPredicate>(ctx, lhs_.multi_linestring());
      case ST_MULTIPOLYGON:
        return EvalInner<TPredicate>(ctx, lhs_.multi_polygon());
      default:
        return BooleanVal::null();
    }
  } catch (bg::exception& ex) {
    ctx->SetError(ex.what());
    return BooleanVal::null();
  }
}

// --- Explicit template instantiations ---

#define DEFINE_RELATION_PREDICATE(relation_name)                                  \
template BooleanVal RelationWrapperWkb::EvalWkbWkb<relation_name##Predicate>(     \
    FunctionContext*, const StringVal&, const StringVal&);

DEFINE_RELATION_PREDICATE(Contains)
DEFINE_RELATION_PREDICATE(Crosses)
DEFINE_RELATION_PREDICATE(Disjoint)
DEFINE_RELATION_PREDICATE(Equals)
DEFINE_RELATION_PREDICATE(Intersects)
DEFINE_RELATION_PREDICATE(Overlaps)
DEFINE_RELATION_PREDICATE(Touches)
DEFINE_RELATION_PREDICATE(Within)

#undef DEFINE_RELATION_PREDICATE

} // namespace impala::geo
