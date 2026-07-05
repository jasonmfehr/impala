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

#include <cstdint>

#include "exprs/geo/boost-common.h"

namespace impala::geo {

// Ported from:
// https://github.com/Esri/spatial-framework-for-hadoop/blob/7226df669cbfaaf1edbfac0461acd1af45e12b81/hive/src/main/java/com/esri/hadoop/hive/BinUtils.java
struct BinStructure {
  double extent_max;
  double extent_min;
  int64_t num_cols;
};

inline BinStructure calculateBinStructure(double bin_size) {
  BinStructure res;
  const int64_t max_bins_per_axis = std::sqrt(std::numeric_limits<int64_t>::max());
  const double size = (bin_size < 1) ? max_bins_per_axis * bin_size : max_bins_per_axis;
  res.extent_max = size / 2;
  res.extent_min = res.extent_max - size;
  res.num_cols = std::ceil(size / bin_size);
  return res;
}

inline int64_t getBinId(double bin_size, double x, double y) {
  BinStructure bin_struct = calculateBinStructure(bin_size);
  const int64_t down = (bin_struct.extent_max - y) / bin_size;
  const int64_t over = (x - bin_struct.extent_min) / bin_size;
  return (down * bin_struct.num_cols) + over;
}

inline box2d getBinEnvelope(double bin_size, int64_t bin_id) {
  BinStructure bin_struct = calculateBinStructure(bin_size);
  const int64_t down = bin_id / bin_struct.num_cols;
  const int64_t over = bin_id % bin_struct.num_cols;
  const double xmin = bin_struct.extent_min + (over * bin_size);
  const double xmax = xmin + bin_size;
  const double ymax = bin_struct.extent_max - (down * bin_size);
  const double ymin = ymax - bin_size;
  return box2d(point2d(xmin, ymin), point2d(xmax, ymax));
}

inline box2d getBinEnvelope(double bin_size, double x, double y) {
  BinStructure bin_struct = calculateBinStructure(bin_size);
  const double down = (bin_struct.extent_max - y) / bin_size;
  const double over = (x - bin_struct.extent_min) / bin_size;
  const double xmin = bin_struct.extent_min + (over * bin_size);
  const double xmax = xmin + bin_size;
  const double ymax = bin_struct.extent_max - (down * bin_size);
  const double ymin = ymax - bin_size;
  return box2d(point2d(xmin, ymin), point2d(xmax, ymax));
}

} // namespace impala::geo
