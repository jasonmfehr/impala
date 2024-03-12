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

#include <functional>
#include <set>
#include <string>
#include <sstream>

using std::function;
using std::set;
using std::string;
using std::basic_stringstream;

namespace impala {

// Joins together complex data types with a specified delimiter.  The complex data types
// are converted to a string by calling the provided predicate function on each object.
//
// The type 'Item' can be any type that has a 'ToString()' function on it where that
// function returns a string, takes no input, and is constant.  The type 'Container' must
// be an object that has the methods 'cbegin()', 'cend()', and 'size()'.
template <typename Container>
string JoinToString(const Container& container, const string delimiter) {
  if (container.size() == 0) {
    return "";
  }

  basic_stringstream<char> s;

  auto iter = container.cbegin();
  for (int i = 0; i < container.size()-1; i++) {
    s << iter->ToString() << delimiter;
    iter++;
  }

  s << iter->ToString();

  return s.str();
}
} // namespace impala
