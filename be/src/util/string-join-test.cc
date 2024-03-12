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

#include "util/string-join.h"

#include <functional>
#include <set>
#include <string>

#include <gutil/strings/strcat.h>

#include "testutil/gtest-util.h"

using namespace std;

namespace impala {

struct _TestStruct {
  string first;
  int second;
  float third;

  _TestStruct(string f, int s, float t) : first(f), second(s), third(t) {}

  bool operator<(const _TestStruct& other) const {
    return first.compare(other.first) < 0 && second < other.second && third < other.third;
  }

  string ToString() const {
    return StrCat(first, "-", second, "-", third);
  }
};

static set<_TestStruct> _buildTestFixture() {
  return set<_TestStruct>{
      _TestStruct("third", 44, 5.3638),
      _TestStruct("first", 42, 3.14159),
      _TestStruct("second", 43, 4.2527)};
}

TEST(JoinToStringTest, EmptyInput) {
  string actual = JoinToString(set<_TestStruct>(), ", ");
  EXPECT_EQ("", actual);
}

TEST(JoinToStringTest, BlankSep) {
  string actual = JoinToString(_buildTestFixture(), "");
  EXPECT_EQ("first-42-3.14159second-43-4.2527third-44-5.3638", actual);
}

TEST(JoinToStringTest, OneCharSep) {
  string actual = JoinToString(_buildTestFixture(), ",");
  EXPECT_EQ("first-42-3.14159,second-43-4.2527,third-44-5.3638", actual);
}

TEST(JoinToStringTest, MultipleCharSep) {
  string actual = JoinToString(_buildTestFixture(), ", ");
  EXPECT_EQ("first-42-3.14159, second-43-4.2527, third-44-5.3638", actual);
}

} //namespace impala
