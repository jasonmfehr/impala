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

from __future__ import absolute_import, division, print_function

from re import search

from SystemTables.ttypes import TQueryTableColumn
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.workload_management import assert_query


class TestWorkloadManagementInit(CustomClusterTestSuite):

  """Tests for the workload management initialization process. The setup method of this
     class waits for the workload management init process to complete before allowing any
     tests to run. """

  WM_DB = "sys"
  QUERY_TBL_LOG_NAME = "impala_query_log"
  QUERY_TBL_LOG = "{0}.{1}".format(WM_DB, QUERY_TBL_LOG_NAME)
  QUERY_TBL_LIVE_NAME = "impala_query_live"
  QUERY_TBL_LIVE = "{0}.{1}".format(WM_DB, QUERY_TBL_LIVE_NAME)

  def setup_method(self, method):
    super(TestWorkloadManagementInit, self).setup_method(method)
    self.wait_for_wm_init_complete()

  def drop_wm_tables(self):
    client = self.create_impala_client()
    assert client.execute("drop table if exists {} purge"
        .format(self.QUERY_TBL_LOG)).success
    assert client.execute("drop table if exists {} purge"
        .format(self.QUERY_TBL_LIVE)).success

  def restart_cluster(self, schema_version, wait_for_init_complete=True, cluster_size=3,
      additional_impalad_opts="", drop_wm_tables=False, wait_for_backends=True):
    """Restarts the Impala cluster. If wait_for_init_complete is True, this function
        blocks until the workload management init process completes. If
        additional_impalad_opts is specified, that string is appended to the impala_args
        startup flag."""
    if drop_wm_tables:
      self.drop_wm_tables()

    self._start_impala_cluster(options=['--impalad_args=--enable_workload_mgmt '
        '--workload_mgmt_schema_version={} -v=2 {}'
        .format(schema_version, additional_impalad_opts),
        '--catalogd_args=--enable_workload_mgmt=true'], cluster_size=cluster_size,
        expected_num_impalads=cluster_size, num_coordinators=cluster_size-1,
        wait_for_backends=wait_for_backends)

    if wait_for_init_complete:
      self.wait_for_wm_init_complete()

  def check_schema_version(self, schema_version):
    """Asserts that all workload management tables are at the specified schema version."""
    for tbl_name in (self.QUERY_TBL_LOG, self.QUERY_TBL_LIVE):
      res = self.create_impala_client().execute("describe extended {}".format(tbl_name))
      assert res.success

      found = False
      version_1_1_0_fields_found = False

      for line in res.data:
        if search(r"schema_version\s+{}".format(schema_version), line):
          found = True
        elif search("_columns", line):
          version_1_1_0_fields_found = True

      assert found, "did not find expected table schema '{}' on table '{}'" \
          .format(schema_version, tbl_name)

      if schema_version == "1.0.0":
        assert not version_1_1_0_fields_found, "found fields that are part of version " \
            "'1.1.0' schema but expected schema version was '1.0.0'"
      elif schema_version == "1.1.0":
        assert version_1_1_0_fields_found, "did not find expected fields that are part " \
            "of version 1.1.0 schema"

  def check_coord_logs_info_fatal(self, log_re):
    """Asserts the provided regex is contained in all coordinator INFO and FATAL logs."""
    for log in ("INFO", "FATAL"):
      self.assert_all_coords_log_contains(log, log_re)

  @CustomClusterTestSuite.with_args(cluster_size=1, impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt")
  def test_start_invalid_version(self):
    """Asserts that starting a cluster with an invalid workload management version
       errors. Cluster sizes of 1 are used to speed up the initial setup."""
    self.check_schema_version("1.1.0")
    self.restart_cluster("foo", False, wait_for_backends=False)

    self.assert_all_coords_log_contains("INFO", "Starting workload management "
        "initialization process")
    self.check_coord_logs_info_fatal("Invalid workload management schema version 'foo'")

  @CustomClusterTestSuite.with_args(cluster_size=1, impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt")
  def test_start_unknown_version(self):
    """Asserts that starting a cluster with an unknown workload management version errors.
       Cluster sizes of 1 are used to speed up the initial setup."""
    invalid_version = "1.0.1"

    self.check_schema_version("1.1.0")
    self.restart_cluster(invalid_version, False, wait_for_backends=False)

    self.assert_all_coords_log_contains("INFO", "Starting workload management "
        "initialization process")
    self.check_coord_logs_info_fatal("Workload management schema version '{}' is not "
        "one of the known versions: '1.0.0', '1.1.0'".format(invalid_version))

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.1.0",
      catalogd_args="--enable_workload_mgmt")
  def test_no_upgrade(self):
    """Tests that no upgrade happens when starting a cluster where the workload management
       tables are already at version 1.1.0."""
    self.restart_cluster("1.1.0")
    self.check_schema_version("1.1.0")
    self.assert_all_coords_log_contains("INFO", "Upgrading workload management table ",
        expected_count=0)

  @CustomClusterTestSuite.with_args(cluster_size=1, impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt")
  def test_create_on_version_1_1_0(self):
    """Asserts that workload management tables are properly created on version 1.1.0 using
       a 10 node cluster when no tables exist.
       Cluster sizes of 1 are used to speed up the initial setup."""
    self.restart_cluster("1.1.0", cluster_size=10, drop_wm_tables=True)
    self.check_schema_version("1.1.0")

  @CustomClusterTestSuite.with_args(cluster_size=1, impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt")
  def test_create_on_version_1_0_0(self):
    """Asserts that workload management tables are properly created on version 1.0.0 using
       a 10 node cluster when no tables exist.
       Cluster sizes of 1 are used to speed up the initial setup."""
    self.restart_cluster("1.0.0", cluster_size=10, drop_wm_tables=True)
    self.check_schema_version("1.0.0")

  @CustomClusterTestSuite.with_args(cluster_size=1, impalad_args="--enable_workload_mgmt",
      catalogd_args="--enable_workload_mgmt")
  def test_upgrade_1_0_0_to_1_1_0(self):
    """Asserts that an upgrade from version 1.0.0 to 1.1.0 succeeds on a 10 node cluster
       when starting with no existing workload management tables."""
    self.restart_cluster("1.0.0", cluster_size=10, drop_wm_tables=True, additional_impalad_opts="-v=2")

    # Veriy the initial table create on version 1.0.0 succeeded.
    self.check_schema_version("1.0.0")
    self.assert_all_coords_log_contains("WARNING", r"Target schema version '1.0.0' is "
        r"not the latest schema version '\d+\.\d+\.\d+'")

    self.restart_cluster("1.1.0", cluster_size=10)
    self.check_schema_version("1.1.0")

    # Assert only one coordinator ran the upgrade process.
    self.assert_one_coord_log_contains("INFO", r"Creating workload management table '{}' "
        r"on schema version '1.1.0'".format(self.QUERY_TBL_LIVE))
    self.assert_one_coord_log_contains("INFO", r"Upgrading workload management table "
        r"'{}' from schema version '1.0.0' to '1.1.0'".format(self.QUERY_TBL_LOG))

  # TODO: run this test for the live table
  @CustomClusterTestSuite.with_args(
      cluster_size=1, catalogd_args="--enable_workload_mgmt",
      impalad_args="--enable_workload_mgmt --workload_mgmt_schema_version=1.1.0")
  def test_log_table_newer_schema_version(self):
    """Asserts a coordinator startup flag version that is older than the workload
       management table schema version will write only the fields associated with the
       startup flag version."""
    self.restart_cluster("1.0.0", cluster_size=1,
        additional_impalad_opts="--query_log_write_interval_s=1")

    # The workload management tables will be on schema version 1.1.0.
    self.check_schema_version("1.1.0")

    # The workload management processing will be running on schema version 1.0.0.
    for table in (self.QUERY_TBL_LOG, self.QUERY_TBL_LOG):
      self.assert_all_coords_log_contains("INFO", "Target schema version '1.0.0' of the "
          "'{}' table is lower than the actual schema version '1.1.0'"
          .format(table))

    # Run a query and ensure it does not populate version 1.1.0 fields.
    client = self.create_impala_client()
    res = client.execute("select * from functional.alltypes")
    assert res.success

    impalad = self.cluster.get_first_impalad()
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)
    assert_query(self.QUERY_TBL_LOG, client, "", impalad=impalad, query_id=res.query_id,
        expected_overrides={
          TQueryTableColumn.SELECT_COLUMNS: "NULL",
          TQueryTableColumn.WHERE_COLUMNS: "NULL",
          TQueryTableColumn.JOIN_COLUMNS: "NULL",
          TQueryTableColumn.AGGREGATE_COLUMNS: "NULL",
          TQueryTableColumn.ORDERBY_COLUMNS: "NULL"})
