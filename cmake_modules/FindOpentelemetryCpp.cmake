##############################################################################
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
##############################################################################

# - Find OpentelemetryCpp static libraries for the OpenTelemetry C++ SDK.
# OPENTELEMETRY_CPP_ROOT hints the location
#
# This module defines
# OPENTELEMETRY_CPP_INCLUDE_DIR, directory containing headers
#
# See https://github.com/open-telemetry/opentelemetry-cpp/blob/main/INSTALL.md for
# documentation on the OpenTelemetry C++ SDK installation.

set(_OPENTELEMETRY_CPP_STATIC_LIBS_DIR ${OPENTELEMETRY_CPP_ROOT}/lib)

set(OPENTELEMETRY_CPP_INCLUDE_DIR "${OPENTELEMETRY_CPP_ROOT}/include")

# Check OPENTELEMETRY_CPP_INCLUDE_DIR by looking for a common header file.
if(NOT EXISTS "${OPENTELEMETRY_CPP_INCLUDE_DIR}/opentelemetry/sdk/resource/resource.h")
  message(FATAL_ERROR "OpentelemetryCpp headers not found at "
    "${OPENTELEMETRY_CPP_INCLUDE_DIR}")
  set(OPENTELEMETRY_CPP_FOUND FALSE)
endif()

find_package(opentelemetry-cpp CONFIG REQUIRED
  COMPONENTS
    api
    sdk
    ext_common
    ext_http_curl
    exporters_otlp_common
    exporters_otlp_file
    exporters_otlp_http
  PATHS
    ${_OPENTELEMETRY_CPP_STATIC_LIBS_DIR}
    ${_OPENTELEMETRY_CPP_STATIC_LIBS_DIR}64
  PATH_SUFFIXES "cmake/opentelemetry-cpp")

mark_as_advanced(
  OPENTELEMETRY_CPP_INCLUDE_DIR
)
