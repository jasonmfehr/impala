# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Target including all protobuf-generated code.
add_custom_target(kudu-rpc-proto-deps)

#### Global header protobufs
PROTOBUF_GENERATE_CPP(
  RPC_HEADER_PROTO_SRCS RPC_HEADER_PROTO_HDRS RPC_HEADER_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES rpc_header.proto)
ADD_EXPORTABLE_LIBRARY(rpc_header_proto
  SRCS ${RPC_HEADER_PROTO_SRCS}
  DEPS protobuf pb_util_proto token_proto
  NONLINK_DEPS ${RPC_HEADER_PROTO_TGTS})

add_dependencies(kudu-rpc-proto-deps ${RPC_HEADER_PROTO_TGTS})

PROTOBUF_GENERATE_CPP(
  RPC_INTROSPECTION_PROTO_SRCS RPC_INTROSPECTION_PROTO_HDRS RPC_INTROSPECTION_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES rpc_introspection.proto)
set(RPC_INTROSPECTION_PROTO_LIBS
  rpc_header_proto
  protobuf)
ADD_EXPORTABLE_LIBRARY(rpc_introspection_proto
  SRCS ${RPC_INTROSPECTION_PROTO_SRCS}
  DEPS ${RPC_INTROSPECTION_PROTO_LIBS}
  NONLINK_DEPS ${RPC_INTROSPECTION_PROTO_TGTS})

add_dependencies(kudu-rpc-proto-deps ${RPC_INTROSPECTION_PROTO_TGTS})

add_definitions(-DKUDU_HEADERS_USE_SHORT_STATUS_MACROS)

### RPC library
set(KRPC_SRCS
    acceptor_pool.cc
    blocking_ops.cc
    client_negotiation.cc
    connection.cc
    connection_id.cc
    constants.cc
    inbound_call.cc
    messenger.cc
    negotiation.cc
    outbound_call.cc
    periodic.cc
    proxy.cc
    reactor.cc
    remote_method.cc
    remote_user.cc
    request_tracker.cc
    result_tracker.cc
    rpc.cc
    rpc_context.cc
    rpc_controller.cc
    rpc_sidecar.cc
    rpc_verification_util.cc
    rpcz_store.cc
    sasl_common.cc
    sasl_helper.cc
    serialization.cc
    server_negotiation.cc
    service_if.cc
    service_pool.cc
    service_queue.cc
    user_credentials.cc
    transfer.cc
)

set(KRPC_LIBS
  cyrus_sasl
  gssapi_krb5
  gutil
  kudu_util
  libev
  rpc_header_proto
  rpc_introspection_proto
  security)

ADD_EXPORTABLE_LIBRARY(krpc
  SRCS ${KRPC_SRCS}
  DEPS ${KRPC_LIBS})

### RPC generator tool
add_executable(protoc-gen-krpc protoc-gen-krpc.cc
  # Impala - add stub for kudu::VersionInfo
  ${CMAKE_CURRENT_SOURCE_DIR}/../../common/kudu_version.cc
  # Impala - add definition for any flag names shared between Impala / Kudu.
  # TODO: Consider either removing code that depends on these flags, or namespacing them
  # somehow.
  ${CMAKE_CURRENT_SOURCE_DIR}/../../common/global-flags.cc)
# IMPALA-8642: kudu_version.cc depends on gen-cpp/Status_types.h in target thrift-deps
add_dependencies(protoc-gen-krpc thrift-deps)
target_link_libraries(protoc-gen-krpc gutil glog gflags protoc protobuf rpc_header_proto ${KUDU_BASE_LIBS})

### RPC test
if(NOT NO_TESTS)
  PROTOBUF_GENERATE_CPP(
    RPC_TEST_DIFF_PACKAGE_SRCS RPC_TEST_DIFF_PACKAGE_HDRS RPC_TEST_DIFF_PACKAGE_TGTS
    SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
    BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
    PROTO_FILES rtest_diff_package.proto)
  add_library(rtest_diff_package_proto ${RPC_TEST_DIFF_PACKAGE_SRCS} ${RPC_TEST_DIFF_PACKAGE_HDRS})
  target_link_libraries(rtest_diff_package_proto rpc_header_proto)

  KRPC_GENERATE(
    RTEST_KRPC_SRCS RTEST_KRPC_HDRS RTEST_KRPC_TGTS
    SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
    BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
    PROTO_FILES rtest.proto)
  add_library(rtest_krpc ${RTEST_KRPC_SRCS} ${RTEST_KRPC_HDRS})
  target_link_libraries(rtest_krpc
    krpc
    rpc_header_proto
    rtest_diff_package_proto)
endif()

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(
  krpc
  mini_kdc
  rpc_header_proto
  rtest_krpc
  security_test_util
  kudu_jwt_util
  jwt_test_certs)
ADD_KUDU_TEST(exactly_once_rpc-test PROCESSORS 10)
ADD_KUDU_TEST(mt-rpc-test RUN_SERIAL true)
ADD_KUDU_TEST(negotiation-test)
ADD_KUDU_TEST(periodic-test)
ADD_KUDU_TEST(proxy-test)
ADD_KUDU_TEST(reactor-test)
ADD_KUDU_TEST(request_tracker-test)
ADD_KUDU_TEST(rpc-bench RUN_SERIAL true)
ADD_KUDU_TEST(rpc-test NUM_SHARDS 8)
ADD_KUDU_TEST(rpc_stub-test)
ADD_KUDU_TEST(service_queue-test RUN_SERIAL true)
