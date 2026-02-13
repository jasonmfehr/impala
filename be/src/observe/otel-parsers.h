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

#include <functional>
#include <string>

#include <boost/algorithm/string/trim.hpp>
#include <glog/logging.h>
#include <gutil/strings/split.h>

namespace impala {

inline void parse_otel_additional_headers(const std::string& unparsed_headers,
		const std::function<void(const std::string&, const std::string&)>& on_header) {
	if (unparsed_headers.empty()) {
    return;
  }

  DCHECK(on_header != nullptr);
	for (auto header : strings::Split(unparsed_headers, ":::")) {
		auto pos = header.find('=');
		const std::string key = boost::algorithm::trim_copy(
				header.substr(0, pos).as_string());
		const std::string value = boost::algorithm::trim_copy(
				header.substr(pos + 1).as_string());

		on_header(key, value);
	}
}

} // namespace impala

