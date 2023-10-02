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

#include "gen-cpp/Types_types.h"

#include "testutil/gtest-util.h"
#include "util/network-util.h"

namespace impala {

TEST(NetworkUtil, NetAddrCompHostnameDiff) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("aaaa");
  first.__set_uds_address("uds");
  first.__set_port(0);

  second.__set_hostname("bbbb");
  second.__set_uds_address("uds");
  second.__set_port(0);

  ASSERT_TRUE(fixture(first, second));
  swap(first, second);
  ASSERT_FALSE(fixture(first, second));
}

TEST(NetworkUtil, NetAddrCompUDSAddrDiff) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_uds_address("aaaa");
  first.__set_port(0);

  second.__set_hostname("host");
  second.__set_uds_address("bbbb");
  second.__set_port(0);

  ASSERT_TRUE(fixture(first, second));
  swap(first, second);
  ASSERT_FALSE(fixture(first, second));
}

TEST(NetworkUtil, NetAddrCompPortDiff) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_uds_address("uds");
  first.__set_port(0);

  second.__set_hostname("host");
  second.__set_uds_address("uds");
  second.__set_port(1);

  ASSERT_TRUE(fixture(first, second));
  swap(first, second);
  ASSERT_FALSE(fixture(first, second));
}

TEST(NetworkUtil, NetAddrCompPortSame) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_uds_address("uds");
  first.__set_port(0);

  second.__set_hostname("host");
  second.__set_uds_address("uds");
  second.__set_port(0);

  ASSERT_FALSE(fixture(first, second));
  swap(first, second);
  ASSERT_FALSE(fixture(first, second));
}

} // namespace impala
