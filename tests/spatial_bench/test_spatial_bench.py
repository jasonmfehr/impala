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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfApacheHive
from tests.common.test_dimensions import create_single_exec_option_dimension

import pytest


@SkipIfApacheHive.feature_not_supported
class TestSpatialBench(ImpalaTestSuite):
  """Runs queries from the Apache Sedona SpatialBench suite."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpatialBench, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def get_workload(cls):
    return "spatial_bench"

  @classmethod
  def get_scale_factor(cls):
    assert False, "get_scale_factor() not implemented"

  @classmethod
  def query(cls, query_num):
    return "{}/q{}".format(cls.get_scale_factor(), query_num)


class TestSpatialBenchScaleFactor1(TestSpatialBench):
  """Runs SpatialBench using scale factor 1."""

  @classmethod
  def get_scale_factor(cls):
    return "sf1"

  def test_q1(self, vector):
    self.run_test_case(self.query(1), vector, use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="Causes OOM")
  def test_q2(self, vector):
    self.run_test_case(self.query(2), vector, use_db='spatial_bench')

  def test_q3(self, vector):
    self.run_test_case(self.query(3), vector, use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="Causes OOM")
  def test_q4(self, vector):
    self.run_test_case(self.query(4), vector, use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="Actual results do not match expected")
  def test_q5(self, vector):
    self.run_test_case(self.query(5), vector, use_db='spatial_bench')
