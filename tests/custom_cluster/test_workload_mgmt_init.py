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

from threading import Thread

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from time import sleep


class TestWorkloadManagementInitBase(CustomClusterTestSuite):

  """Base class for all tests that assert workload management initialization behavior.
     The setup method of this class waits for the workload management init process to
     complete before allowing any tests to run. """

  WM_INIT_COMPLETE_LOG = "Completed workload management initialization"
  WM_DB = "sys"
  QUERY_TBL_LOG = "{0}.impala_query_log".format(WM_DB)
  QUERY_TBL_LIVE = "{0}.impala_query_live".format(WM_DB)

  @classmethod
  def add_test_dimensions(cls):
    super(TestWorkloadManagementInitBase, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  def setup_method(self, method):
    super(TestWorkloadManagementInitBase, self).setup_method(method)

    self.assert_all_impalad_log_contains("INFO", self.WM_INIT_COMPLETE_LOG, timeout_s=120)


class TestWorkloadManagementInit(TestWorkloadManagementInitBase):
  """Tests the workload management initialization process. This process requires
     coordination between all coordinators to ensure that only 1 coordinator runs the
     initialization process that creates and maintains the database tables. This
     coordinator is known as the lead coordinator. The other coordinators wait until the
     lead coordinator signals that it has finished workload management initialization
     before continuing with their own much shorter initialization process.

     The init process also handles creating and updating the workload management db
     tables. If no action is necessary, then no DDLs are executed."""

  NONLEADER_LOG = "Starting workload management thread without setting up the workload " \
                  "management database tables"
  LEADER_LOG = "Starting workload management initialization that will set up the " \
               "workload management database tables"

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_1 -v=2",
      catalogd_args="--enable_workload_mgmt", cluster_size=3,
      num_exclusive_coordinators=3)
  def test_start_3_node_cluster(self, vector):
    """Asserts that a cluster consisting of only 3 coordinators starts correctly with
       only one coordinator running workload management initialization."""

    matches = []
    for i in range(0, self.get_impalad_cluster_size()):
      daemon_name = "impalad_node{}".format(i)
      if i == 0:
        daemon_name = "impalad"

      matches.append(self.assert_log_contains(daemon_name, "INFO",
          r'{0}|{1}'.format(self.NONLEADER_LOG, self.LEADER_LOG), timeout_s=1))

    leader_cnt = 0
    nonleader_cnt = 0
    for i in range(0, self.get_impalad_cluster_size()):
      match_txt = matches[i].group(0)
      if match_txt == self.NONLEADER_LOG:
        nonleader_cnt += 1
      elif match_txt == self.LEADER_LOG:
        leader_cnt += 1
      else:
        assert False, "unknown match: {}".format(match_txt)

    assert leader_cnt == 1
    assert nonleader_cnt == self.get_impalad_cluster_size() - 1

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_2 -v=2",
      catalogd_args="--enable_workload_mgmt", cluster_size=1,
      num_exclusive_coordinators=1)
  def test_start_1_node_cluster_add_another_coordinator(self, vector):
    """Asserts that a single node coordinator only cluster starts successfully and that
       another coordinator can be added after workload management completes."""

    self._start_impala_cluster(cluster_size=2, expected_num_impalads=2,
        options=['--add_coordinators',
        '--impalad_args=--enable_workload_mgmt --cluster_id=test_wm_init_2 -v=2'])

    self.assert_log_contains("impalad_node1", "INFO", self.NONLEADER_LOG, timeout_s=30)
    self.assert_log_contains("impalad_node1", "INFO", self.WM_INIT_COMPLETE_LOG,
        timeout_s=30)
    self.assert_log_contains("impalad_node1", "INFO", self.LEADER_LOG, expected_count=0,
        timeout_s=30)

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_3 -v=2",
      catalogd_args="--enable_workload_mgmt", cluster_size=3,
      num_exclusive_coordinators=3)
  def test_start_3_node_cluster_add_another_coordinator(self, vector):
    """Asserts that a 3 node coordinator only cluster starts successfully and that another
       coordinator can be added to the cluster after workload management completes."""

    self._start_impala_cluster(cluster_size=4, expected_num_impalads=4,
        options=['--add_coordinators',
        '--impalad_args=--enable_workload_mgmt --cluster_id=test_wm_init_3 -v=2'])

    self.assert_log_contains("impalad_node3", "INFO", self.NONLEADER_LOG, timeout_s=30)
    self.assert_log_contains("impalad_node3", "INFO", self.WM_INIT_COMPLETE_LOG,
        timeout_s=30)
    self.assert_log_contains("impalad_node3", "INFO", self.LEADER_LOG, expected_count=0,
        timeout_s=30)

    # Assert the other daemons did not re-run workload management initialization. The
    # default value '1' of the expected_count parameter ensures the original log messages
    # are the only messages present in the log.
    self.assert_all_impalad_log_contains("INFO",
          r'{0}|{1}'.format(self.NONLEADER_LOG, self.LEADER_LOG), timeout_s=1)


class TestWorkloadManagementTableSchema(TestWorkloadManagementInitBase):
  """Tests that assert the workload management db table schema management."""

  def setup_method(self, method):
    super(TestWorkloadManagementTableSchema, self).setup_method(method)

    self.client.execute("drop table if exists {}".format(self.QUERY_TBL_LOG))
    self.client.execute("drop table if exists {}".format(self.QUERY_TBL_LIVE))

  def restart_cluster(self, cluster_id, schema_version="1.0.0", debug_actions="",
      wait_for_init_complete=True, cluster_size=1, additional_options=""):
    self._start_impala_cluster(options=['--impalad_args=--enable_workload_mgmt '
        '--cluster_id={} --workload_mgmt_schema_version={} --debug_actions={} -v=2 {}'
        .format(cluster_id, schema_version, debug_actions, additional_options),
        '--catalogd_args=--enable_workload_mgmt=true'], cluster_size=cluster_size,
        expected_num_impalads=cluster_size)

    if wait_for_init_complete:
      self.assert_all_impalad_log_contains("INFO", self.WM_INIT_COMPLETE_LOG,
          timeout_s=120)

  def assert_logs(self, actual_schema_ver, target_schema_ver):
    """After workload management initialization has completed, check the level 2 info logs
       to ensure the correct actions were taken during the init process."""
    self.assert_impalad_log_contains("INFO", r"Target workload management schema version "
        r"is '{}'".format(target_schema_ver))

    upgrade_expect_count = 0
    if actual_schema_ver != target_schema_ver and actual_schema_ver != "":
      upgrade_expect_count = 1

    # Query Log Table - This table is always created on version 1.0.0 if it does not exist
    # and upgraded only if the schema versions have changed.
    create_expect_count = 0
    if actual_schema_ver != target_schema_ver and actual_schema_ver != "1.0.0":
      create_expect_count = 1
    self.assert_impalad_log_contains("INFO", r"Actual current workload management schema "
        r"version of the '{}' table is '{}'"
        .format(self.QUERY_TBL_LOG, actual_schema_ver))
    self.assert_impalad_log_contains("INFO", r"Creating workload management table '{}' "
        r"on schema version '1.0.0'".format(self.QUERY_TBL_LOG), create_expect_count)
    self.assert_impalad_log_contains("INFO", r"Upgrading workload management table '{}' "
        r"from schema version '{}' to '{}'".format(self.QUERY_TBL_LOG, actual_schema_ver,
        target_schema_ver), upgrade_expect_count)

    # Query Live Table - This table is never upgraded and always created on the target
    # schema version.
    create_expect_count = 0
    if actual_schema_ver != target_schema_ver: create_expect_count = 1
    self.assert_impalad_log_contains("INFO", r"Actual current workload management schema "
        r"version of the '{}' table is '{}'"
        .format(self.QUERY_TBL_LIVE, actual_schema_ver))
    self.assert_impalad_log_contains("INFO", r"Creating workload management table '{}' "
        r"on schema version '{}'".format(self.QUERY_TBL_LIVE, target_schema_ver),
        create_expect_count)
    self.assert_impalad_log_contains("INFO", r"Upgrading workload management table '{}'"
        .format(self.QUERY_TBL_LIVE), 0)

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_4 -v=2",
      catalogd_args="--enable_workload_mgmt", cluster_size=1)
  def test_version_1_tables_not_exist(self, vector):
    """Asserts that workload management tables are created at schema version 1.0.0 and not
       upgraded or altered."""
    self.restart_cluster(cluster_id="test_wm_init_4")
    self.assert_logs(actual_schema_ver="", target_schema_ver="1.0.0")

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_5 -v=2",
      catalogd_args="--enable_workload_mgmt", cluster_size=1)
  def test_version_1_tables_exist(self, vector):
    """Asserts that workload management tables already at schema version 1.0.0 are not
       altered at cluster startup."""
    self.restart_cluster(cluster_id="test_wm_init_5")
    self.assert_logs(actual_schema_ver="", target_schema_ver="1.0.0")

    self.restart_cluster(cluster_id="test_wm_init_5")
    self.assert_logs(actual_schema_ver="1.0.0", target_schema_ver="1.0.0")

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_6 -v=2",
      catalogd_args="--enable_workload_mgmt",
      cluster_size=1)
  def test_version_2_tables_not_exist(self, vector):
    """Asserts that workload management tables are created at schema version 1.1.0 and not
       upgraded or altered."""
    self.restart_cluster(cluster_id="test_wm_init_6", schema_version="1.1.0")
    self.assert_logs(actual_schema_ver="", target_schema_ver="1.1.0")

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_7 -v=2",
      catalogd_args="--enable_workload_mgmt",
      cluster_size=1)
  def test_version_2_upgrade(self, vector):
    """Asserts that the workload management tables already at schema version 1.0.0 are
       upgraded to schema version 1.1.0."""
    self.restart_cluster(cluster_id="test_wm_init_7", schema_version="1.0.0")
    self.assert_logs(actual_schema_ver="", target_schema_ver="1.0.0")

    self.restart_cluster(cluster_id="test_wm_init_7", schema_version="1.1.0")
    self.assert_logs(actual_schema_ver="1.0.0", target_schema_ver="1.1.0")

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_8 -v=2",
      catalogd_args="--enable_workload_mgmt",
      cluster_size=1)
  def test_version_2_tables_not_upgrade(self, vector):
    """Asserts that the workload management tables already at schema version 1.1.0 are not
       altered at cluster startup."""
    self.restart_cluster(cluster_id="test_wm_init_8", schema_version="1.1.0")
    self.assert_logs(actual_schema_ver="", target_schema_ver="1.1.0")

    self.restart_cluster(cluster_id="test_wm_init_9", schema_version="1.1.0")
    self.assert_logs(actual_schema_ver="1.1.0", target_schema_ver="1.1.0")

  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_workload_mgmt --cluster_id=test_wm_init_9 -v=2 ",
      catalogd_args="--enable_workload_mgmt", cluster_size=1)
  def test_start_cluster_slow_statestore(self, vector):
    """Asserts that workload management properly initializes even if the lead coordinator
       negotiation takes several minutes. Queries should queue up in memory until the init
       process completes then write out to the completed queries table. The live queries
       table should be queryable while the init process is running."""
    self.restart_cluster(cluster_id="test_wm_init_9", schema_version="1.1.0")
    self.assert_logs(actual_schema_ver="", target_schema_ver="1.1.0")

    def res_cluster():
      self.restart_cluster(cluster_id="test_wm_init_9", schema_version="1.1.0",
          debug_actions="WORKLOAD_MGMT_INIT:SLEEP@30000", wait_for_init_complete=False,
          cluster_size=3, additional_options="--query_log_write_interval_s=1")

    restart_thread = Thread(target=res_cluster)
    restart_thread.start()

    # Give the new daemons some time to start up and change the log file softlinks
    sleep(10)

    # Wait for the local catalog to populate so the cluster can run queries.
    self.assert_all_impalad_log_contains("INFO",
        r'Target workload management schema version', timeout_s=30)

    client = self.create_impala_client()
    try:
      res = client.execute("select count(*) from {}".format(self.QUERY_TBL_LIVE))
      assert res.success
      assert len(res.data) == 1

      restart_thread.join()
      self.assert_all_impalad_log_contains("INFO",
          r'Completed workload management initialization', timeout_s=120)

      # Ensure the query against the live table was written to the log table even though
      # the query was executed during the workload management init process.
      res2 = client.execute("select * from {} where query_id='{}'".format(
          self.QUERY_TBL_LOG, res.query_id))
      assert res2.success
      assert len(res2.data) == 1, "did not find query '{}' in completed queries table" \
          .format(res.query_id)
    finally:
      client.close()
