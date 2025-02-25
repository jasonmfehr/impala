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

#include "kudu/util/version_util.h"

namespace kudu {

// Adds additional comparison operators to the kudu::Version class.

// Compare two Version objects. Versions that contain an extra component sort before
// (less than) versions that do not contain an extra component. The extra component is
// sorted alphabetically without modifying the case. Thus '-SNAPSHOT' sorts as greater
// than '-RELEASE'.
// Example sort order (from least to greatest):
//   1.0.0-RELEASE, 1.0.0-SNAPSHOT, 1.0.0, 1.0.1-SNAPSHOT.
bool operator<(const Version& lhs, const Version& rhs);
bool operator<=(const Version& lhs, const Version& rhs);
bool operator>(const Version& lhs, const Version& rhs);
bool operator!=(const Version& lhs, const Version& rhs);

} // namespace kudu

namespace impala {

// Constructor function that allows for setting the values of some struct members.
kudu::Version ConstructVersion(int maj, int min, int maint);

} // namespace impala
