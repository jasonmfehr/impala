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

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include <gflags/gflags_declare.h>

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"

using namespace std;
using namespace impala;
using namespace impala_udf;

DECLARE_int32(geospatial_max_quad_segs);

namespace impala::geo {

namespace {

using TestFunc = const function<void(FunctionContext*, BufferWrapperWkb*, bool)>&;

void RunTestWithContext(StringVal* buffer_style_arg = nullptr,
    TestFunc test_f = nullptr, bool const_distance_arg = true,double distance = 1.0) {
  MemTracker m;
  MemPool pool(&m);
  FunctionContext::TypeDesc return_type;
  vector<AnyVal*> constant_args;
  DoubleVal distance_arg;
  BooleanVal use_spheroid_arg;

  distance_arg.is_null = false;
  distance_arg.val = distance;

  use_spheroid_arg.is_null = false;
  use_spheroid_arg.val = false;

  return_type.type = FunctionContext::Type::TYPE_STRING;

  vector<FunctionContext::TypeDesc> arg_types(4);
  arg_types[0].type = FunctionContext::Type::TYPE_STRING;
  arg_types[1].type = FunctionContext::Type::TYPE_DOUBLE;
  arg_types[2].type = FunctionContext::Type::TYPE_BOOLEAN;
  arg_types[3].type = FunctionContext::Type::TYPE_STRING;

  FunctionContext* ctx = FunctionContextImpl::CreateContext(nullptr, &pool, &pool,
      return_type, arg_types, 0, true);

  constant_args.push_back(nullptr);
  if (const_distance_arg) {
    constant_args.push_back(reinterpret_cast<AnyVal*>(&distance_arg));
  } else {
    constant_args.push_back(nullptr);
  }
  constant_args.push_back(reinterpret_cast<AnyVal*>(&use_spheroid_arg));
  constant_args.push_back(reinterpret_cast<AnyVal*>(buffer_style_arg));
  ctx->impl()->SetConstantArgs(std::move(constant_args));

  BufferWrapperWkb fixture;
  bool init_result = fixture.InitFromPrepareArgs(ctx);

  if (test_f) {
    test_f(ctx, &fixture, init_result);
  } else {
    EXPECT_TRUE(init_result);
    EXPECT_FALSE(ctx->has_error());
    EXPECT_STREQ(nullptr, ctx->error_msg());
  }

  ctx->impl()->Close();
  delete ctx;
  pool.FreeAll();
}

StringVal CreateStyleArg(string_view style_str) {
  StringVal style_arg;
  style_arg.is_null = false;
  style_arg.ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(style_str.data()));
  style_arg.len = style_str.size();

  return style_arg;
}

} // anonymous namespace

class BufferWrapperWkbTest : public testing::Test {};

TEST_F(BufferWrapperWkbTest, DefaultStrategies) {
  BufferWrapperWkb fixture;

  EXPECT_TRUE(holds_alternative<buff::distance_symmetric<coord_type>>(
      fixture.strategy_distance_));
  buff::distance_symmetric<coord_type> actual_dist =
      get<buff::distance_symmetric<coord_type>>(fixture.strategy_distance_);
  EXPECT_EQ(1, actual_dist.factor());

  EXPECT_TRUE(holds_alternative<buff::join_round>(fixture.strategy_join_));
  EXPECT_TRUE(holds_alternative<buff::end_round>(fixture.strategy_end_));
  EXPECT_TRUE(holds_alternative<buff::point_circle>(fixture.strategy_point_));
}

TEST_F(BufferWrapperWkbTest, UseSpheroidNonConst) {
  MemTracker m;
  MemPool pool(&m);
  FunctionContext::TypeDesc return_type;
  vector<FunctionContext::TypeDesc> arg_types;
  FunctionContext* ctx;
  vector<AnyVal*> constant_args;
  BooleanVal use_spheroid_arg;
  BufferWrapperWkb fixture;

  arg_types.emplace_back();
  arg_types.at(0).type = FunctionContext::Type::TYPE_STRING;
  arg_types.emplace_back();
  arg_types.at(1).type = FunctionContext::Type::TYPE_DOUBLE;
  arg_types.emplace_back();
  arg_types.at(2).type = FunctionContext::Type::TYPE_BOOLEAN;

  return_type.type = FunctionContext::Type::TYPE_STRING;

  ctx = FunctionContextImpl::CreateContext(nullptr, &pool, &pool, return_type, arg_types,
      0, true);

  constant_args.push_back(nullptr);
  constant_args.push_back(nullptr);
  constant_args.push_back(nullptr);
  ctx->impl()->SetConstantArgs(std::move(constant_args));

  EXPECT_FALSE(fixture.InitFromPrepareArgs(ctx));
  EXPECT_TRUE(ctx->has_error());
  EXPECT_STREQ("The 'use_spheroid' argument must be constant.", ctx->error_msg());

  ctx->impl()->Close();
  delete ctx;
}

TEST_F(BufferWrapperWkbTest, BufferStyleNonConst) {
  RunTestWithContext(nullptr, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("The 'buffer_style' argument must be constant.", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleNullStyleArg) {
  StringVal style_arg;

  style_arg.is_null = true;
  style_arg.ptr = nullptr;
  style_arg.len = 0;

  RunTestWithContext(&style_arg);
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleEmptyStyle) {
  const string style_str = "";
  StringVal style_arg = CreateStyleArg(style_str);

  RunTestWithContext(&style_arg);
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleLongestAllowedString) {
  auto flag_setter =
      ScopedFlagSetter<int32_t>::Make(&FLAGS_geospatial_max_quad_segs, 2147483647);

  const string style_str = "endcap=round join=round side=right quad_segs=2147483647 "
      "mitre_limit=-179769000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000000000000000000000000000000000000000000000000";
  StringVal style_arg = CreateStyleArg(style_str);

  RunTestWithContext(&style_arg);
}

TEST_F(BufferWrapperWkbTest, ParseBufferBufferStyleTooLong) {
  string buf_style_str;
  for (int i=0; i<1001; i++) {
    buf_style_str += "a";
  }
  StringVal style = CreateStyleArg(buf_style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Buffer style string is too long. Maximum allowed length is 1000.",
        ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleUnknownKeyError) {
  const string style_str = "foo=bar";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Unknown buffer style key 'foo'", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleNoKeyError) {
  const string style_str = "=bar";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Unknown buffer style key 'bar'", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleQuadSegsInvalidString) {
  const string style_str = "quad_segs=abc";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value 'abc' for quad_segs", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleQuadSegsZero) {
  const string style_str = "quad_segs=0";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '0' for quad_segs", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleQuadSegsNegative) {
  const string style_str = "quad_segs=-1";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '-1' for quad_segs", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleQuadSegsTooLargeDefault) {
  const string style_str = "quad_segs=1000000";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Number of quad segments exceeds the maximum allowed value of 999999.",
        ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleQuadSegsTooLargeCustom) {
  auto flag_setter = ScopedFlagSetter<int32_t>::Make(&FLAGS_geospatial_max_quad_segs, 64);

  const string style_str = "quad_segs=65";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Number of quad segments exceeds the maximum allowed value of 64.",
        ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleQuadSegsEqualsMin) {
  const string style_str = "quad_segs=1";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style);
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleQuadSegsEqualsMax) {
  auto flag_setter = ScopedFlagSetter<int32_t>::Make(&FLAGS_geospatial_max_quad_segs, 64);

  const string style_str = "quad_segs=64";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style);
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleEndcapSquareUnsupported) {
  const string style_str = "endcap=square";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Endcap value of 'square' is not supported.", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleEndcapInvalidValue) {
  const string style_str = "endcap=triangle";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value 'triangle' for endcap", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMiterLimitZero) {
  const string style_str = "miter_limit=0";
  StringVal style_arg = CreateStyleArg(style_str);

  RunTestWithContext(&style_arg);
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMiterLimitNonPositive) {
  const string style_str = "miter_limit=-1";
  StringVal style_arg = CreateStyleArg(style_str);

  RunTestWithContext(&style_arg);
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMiterLimitError) {
  const string style_str = "miter_limit=foo";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value 'foo' for miter_limit", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMitreLimitError) {
  const string style_str = "mitre_limit=foo";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value 'foo' for mitre_limit", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleJoinBevelUnsupported) {
  const string style_str = "join=bevel";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Join value of 'bevel' is not supported.", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleJoinInvalidValue) {
  const string style_str = "join=foo";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value 'foo' for join", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleSideInvalidValue) {
  const string style_str = "side=center";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value 'center' for side", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMissingJoinValue) {
  const string style_str = "join=";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '' for join", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMissingQuadSegsValue) {
  const string style_str = "quad_segs=";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '' for quad_segs", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMissingEndcapValue) {
  const string style_str = "endcap=";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '' for endcap", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMissingMitreLimitValue) {
  const string style_str = "mitre_limit=";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '' for mitre_limit", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMissingMiterLimitValue) {
  const string style_str = "miter_limit=";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '' for miter_limit", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, ParseBufferStyleMissingSideValue) {
  const string style_str = "side=";
  StringVal style = CreateStyleArg(style_str);

  RunTestWithContext(&style, [](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    EXPECT_FALSE(init);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_STREQ("Invalid value '' for side", ctx->error_msg());
  });
}

TEST_F(BufferWrapperWkbTest, NonConstantDistance) {
  const string style_str = "";
  const string wkt_point_str = "POINT (0 0)";
  StringVal wkt_point = wkt_point_str.c_str();
  StringVal style = CreateStyleArg(style_str);
  double distance = 2.0;

  wkt_point.is_null = false;

  RunTestWithContext(&style,
      [&wkt_point, distance](FunctionContext* ctx, BufferWrapperWkb* b, bool init) {
    ASSERT_TRUE(init);
    ASSERT_FALSE(ctx->has_error());

    StringVal result;
    ASSERT_TRUE(b->FromWkt(ctx, wkt_point, OGCType::ST_POINT));
    ASSERT_TRUE(b->Buffer(ctx, distance, &result));
  }, false);
}

} // namespace impala::geo
