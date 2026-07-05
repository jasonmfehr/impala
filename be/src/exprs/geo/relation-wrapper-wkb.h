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

#include "exprs/geo/common.h"
#include "exprs/geo/geometry-wrapper-wkb.h"
#include "udf/udf.h"

namespace impala::geo {

using impala_udf::BooleanVal;

// Evaluates spatial relationship predicates on WKB-encoded geometries.
// Only supports (GEOMETRY, GEOMETRY) input — no STRING overloads in WKB mode.
//
// Allocated once in Prepare and reused across rows to avoid repeated heap
// allocation for the internal geometry vectors. When the LHS is a constant
// expression, it is deserialized once in Prepare and reused for every row.
class RelationWrapperWkb {
 public:
  template <class TPredicate>
  static BooleanVal EvalWkbWkb(
      FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom);

  bool PrepareLhs(FunctionContext* ctx, const StringVal& geom);
  bool PrepareRhs(FunctionContext* ctx, const StringVal& geom);
  bool lhs_prepared() const { return lhs_prepared_; }
  bool rhs_prepared() const { return rhs_prepared_; }

  GeometryWrapperWkb& lhs() { return lhs_; }
  GeometryWrapperWkb& rhs() { return rhs_; }
  OGCType lhs_type() const { return lhs_type_; }
  OGCType rhs_type() const { return rhs_type_; }

 private:
  template <class TPredicate>
  BooleanVal Eval(FunctionContext* ctx);

  template <class TPredicate, class lhs_geometry_t>
  BooleanVal EvalInner(FunctionContext* ctx, const lhs_geometry_t& lhs);

  template <class TPredicate, class lhs_geometry_t, class rhs_geometry_t>
  BooleanVal EvalInner2(FunctionContext* ctx,
      const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);

  bool lhs_prepared_ = false;
  bool rhs_prepared_ = false;
  GeometryWrapperWkb lhs_, rhs_;
  OGCType lhs_type_, rhs_type_;
};

struct RelationPredicate {
  static constexpr bool RESULT_IF_BBOX_DOES_NOT_INTERSECT = false;
};

struct ContainsPredicate : public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct CrossesPredicate : public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct DisjointPredicate : public RelationPredicate {
  static constexpr bool RESULT_IF_BBOX_DOES_NOT_INTERSECT = true;
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct EqualsPredicate : public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct IntersectsPredicate : public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct OverlapsPredicate : public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct TouchesPredicate : public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct WithinPredicate : public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

} // namespace impala::geo
