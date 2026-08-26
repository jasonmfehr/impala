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

from copy import deepcopy


@SkipIfApacheHive.feature_not_supported
class SpatialBenchBase(ImpalaTestSuite):
  """Runs queries from the Apache Sedona SpatialBench suite."""

  @classmethod
  def add_test_dimensions(cls):
    super(SpatialBenchBase, cls).add_test_dimensions()
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

  @classmethod
  def new_vector(cls, vector):
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['batch_size']
    return new_vector

  def test_q1(self, vector):
    self.run_test_case(self.query(1), vector, use_db='spatial_bench')

  def test_q2(self, vector):
    # Runtime: 8m
    self.run_test_case(self.query(2), self.new_vector(vector), use_db='spatial_bench')

  def test_q3(self, vector):
    self.run_test_case(self.query(3), vector, use_db='spatial_bench')

  def test_q4(self, vector):
    # Runtime: 25m
    self.run_test_case(self.query(4), self.new_vector(vector), use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="Requires support for Geometry Collections")
  def test_q5(self, vector):
    self.run_test_case(self.query(5), vector, use_db='spatial_bench')

  def test_q6(self, vector):
    # Runtime: 7m30s
    self.run_test_case(self.query(6), vector, use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="No ST_MakeLine() Function")
  def test_q7(self, vector):
    self.run_test_case(self.query(7), vector, use_db='spatial_bench')

  def test_q8(self, vector):
    self.run_test_case(self.query(8), vector, use_db='spatial_bench')

  def test_q9(self, vector):
    self.run_test_case(self.query(9), vector, use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="Runtime over 2 hours")
  def test_q10(self, vector):
    self.run_test_case(self.query(10), vector, use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="Runtime over 2 hours")
  def test_q11(self, vector):
    self.run_test_case(self.query(11), vector, use_db='spatial_bench')

  @pytest.mark.xfail(run=False, reason="Requires K-Nearest Neighbor Function")
  def test_q12(self, vector):
    self.run_test_case(self.query(12), vector, use_db='spatial_bench')


class TestSpatialBenchScaleFactor01(SpatialBenchBase):
  """Runs SpatialBench using scale factor 0.1."""

  @classmethod
  def get_scale_factor(cls):
    return "sf0.1"


# TODO: Uncomment when ready to run tests at scale factor 1.
# class TestSpatialBenchScaleFactor1(SpatialBenchBase):
#   """Runs SpatialBench using scale factor 1."""

#   @classmethod
#   def get_scale_factor(cls):
#     return "sf1"
