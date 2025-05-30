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

# Impala tests for DDL statements
from __future__ import absolute_import, division, print_function
from builtins import int
import time

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import WAREHOUSE, IS_S3

# Checks different statements' effect on last DDL time and last compute stats time.
class TestLastDdlTimeUpdate(ImpalaTestSuite):

  @classmethod
  def add_test_dimensions(cls):
    super(TestLastDdlTimeUpdate, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

    if cls.exploration_strategy() == 'core' and not IS_S3:
      # Don't run on core.  This test is very slow and we are unlikely
      # to regress here.
      cls.ImpalaTestMatrix.add_constraint(lambda v: False)

  class TableFormat:
    HDFS = 1
    KUDU = 2
    INTEGRATED_ICEBERG = 3
    NON_INTEGRATED_ICEBERG = 4

  # Convenience class to make calls to TestLastDdlTimeUpdate.run_test() shorter by
  # storing common arguments as members and substituting table name and HDFS warehouse
  # path to the query string.
  class TestHelper:
    class TimeState:
      CHANGED = "Changed"
      UNCHANGED = "Unchanged"
      SET = "Set"
      UNSET = "Unset"

    def __init__(self, test_suite, db_name, tbl_name):
      self.test_suite = test_suite
      self.db_name = db_name
      self.tbl_name = tbl_name
      self.fq_tbl_name = self._generate_fq_tbl_name(self.db_name, self.tbl_name)

    def expect_no_time_change(self, query):
      """Neither transient_lastDdlTime or impala.lastComputeStatsTime should be
      changed by running the query.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, self.TimeState.UNCHANGED, self.TimeState.UNCHANGED)

    def expect_ddl_time_change(self, query):
      """Running the query should increase transient_lastDdlTime but
      not impala.lastComputeStatsTime.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, self.TimeState.CHANGED, self.TimeState.UNCHANGED)

    def expect_stat_time_change(self, query):
      """Running the query should increase impala.lastComputeStatsTime but
      not transient_lastDdlTime.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, self.TimeState.UNCHANGED, self.TimeState.CHANGED)

    def expect_ddl_time_change_on_rename(self, new_tbl_name, expect_change):
      """
      Checks that after an ALTER TABLE ... RENAME query transient_lastDdlTime is higher on
      the new table than it was on the old table.
      """
      query = "alter table %(TBL)s rename to {}".format(self.db_name + "." + new_tbl_name)
      if expect_change:
        self.run_test(
            query, self.TimeState.CHANGED, self.TimeState.UNCHANGED, new_tbl_name)
      else:
        self.run_test(
            query, self.TimeState.UNCHANGED, self.TimeState.UNCHANGED, new_tbl_name)

    def expect_stat_time_set(self, query):
      """Running the query should not change transient_lastDdlTime while
      impala.lastComputeStatsTime should be set.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, self.TimeState.UNCHANGED, self.TimeState.SET)

    def expect_stat_time_unset(self, query):
      """Running the query should not change transient_lastDdlTime while
      impala.lastComputeStatsTime should be unset.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      self.run_test(query, self.TimeState.UNCHANGED, self.TimeState.UNSET)

    def run_test(self, query, expected_ddl_time_state, expected_stats_time_state,
        new_tbl_name=None):
      """
      Runs the query and compares the last ddl/compute stats time before and after
      executing the query. If expect_changed_ddl_time or expect_changed_stat_time
      is true, then we expect the given table property to be increased, otherwise we
      expect it to be unchanged.
      If the query renames the table, the new table name has to be provided in
      'new_tbl_name' so that the new table is checked after the query.
      The following strings are substituted in the query: "%(TBL)s" and "%(WAREHOUSE)s"
      """
      # Get timestamps before executing query.
      (beforeDdlTime, beforeStatsTime) = self._get_ddl_and_stats_time(
          self.db_name, self.tbl_name)
      # HMS uses a seconds granularity on the last ddl time - sleeping 1100 ms should be
      # enough to ensure that the new timestamps are strictly greater than the old ones.
      time.sleep (1.1)

      self.test_suite.execute_query(query %
        {'TBL': self.fq_tbl_name, 'WAREHOUSE': WAREHOUSE})
      if new_tbl_name is not None:
        self._update_name(new_tbl_name)

      # Get timestamps after executing query.
      (afterDdlTime, afterStatsTime) = self._get_ddl_and_stats_time(
          self.db_name, self.tbl_name)

      self.check_time_state(expected_ddl_time_state, afterDdlTime, beforeDdlTime)
      self.check_time_state(expected_stats_time_state, afterStatsTime, beforeStatsTime)

    def check_time_state(self, expected_state, after_time, before_time):
      if expected_state == self.TimeState.CHANGED:
        # Check that the new time is strictly greater than the old one.
        assert int(after_time) > int(before_time)
      elif expected_state == self.TimeState.UNCHANGED:
        assert int(after_time) == int(before_time)
      elif expected_state == self.TimeState.SET:
        assert after_time != ""
      elif expected_state == self.TimeState.UNSET:
        assert after_time == ""

    def _update_name(self, new_tbl_name):
      """"
      Update the name, for example because of a table rename ddl. The database name does
      not change.
      """
      self.tbl_name = new_tbl_name
      self.fq_tbl_name = self._generate_fq_tbl_name(self.db_name, self.tbl_name)

    def _generate_fq_tbl_name(self, db_name, tbl_name):
      return "%s.%s" % (db_name, tbl_name)

    def _get_ddl_and_stats_time(self, db_name, tbl_name):
      HIVE_LAST_DDL_TIME_PARAM_KEY = "transient_lastDdlTime"
      LAST_COMPUTE_STATS_TIME_KEY = "impala.lastComputeStatsTime"

      table = self.test_suite.hive_client.get_table(db_name, tbl_name)
      assert table is not None
      ddlTime = table.parameters.get(HIVE_LAST_DDL_TIME_PARAM_KEY, "")
      statsTime = table.parameters.get(LAST_COMPUTE_STATS_TIME_KEY, "")
      return (ddlTime, statsTime)

  def _create_table(self, fq_tbl_name, format):
    if format == self.TableFormat.HDFS:
      self.execute_query("create external table %s (i int) "
          "partitioned by (j int, s string)" % fq_tbl_name)
    elif format == self.TableFormat.KUDU:
      self.execute_query("create table %s (i int primary key) partition by "
          "hash(i) partitions 3 stored as kudu" % fq_tbl_name)
    elif format == self.TableFormat.INTEGRATED_ICEBERG:
      self.execute_query("create table %s (i int) partitioned by (j int, s string) "
          "stored by iceberg" % fq_tbl_name)
    elif format == self.TableFormat.NON_INTEGRATED_ICEBERG:
      self.execute_query("create table %s (i int) partitioned by (j int, s string) "
          "stored by iceberg tblproperties('iceberg.catalog'='hadoop.tables')"
          % fq_tbl_name)

  def _create_and_init_test_helper(self, unique_database, tbl_name, table_format):
    helper = TestLastDdlTimeUpdate.TestHelper(self, unique_database, tbl_name)
    self._create_table(helper.fq_tbl_name, table_format)

    # compute statistics to fill table property impala.lastComputeStatsTime
    self.execute_query("compute stats %s" % helper.fq_tbl_name)
    return helper

  def test_hdfs_alter(self, vector, unique_database):
    TBL_NAME = "alter_test_tbl"
    h = self._create_and_init_test_helper(
        unique_database, TBL_NAME, self.TableFormat.HDFS)

    # add/drop partitions
    h.expect_no_time_change("alter table %(TBL)s add partition (j=1, s='2012')")
    h.expect_no_time_change("alter table %(TBL)s add if not exists "
                            "partition (j=1, s='2012')")
    h.expect_no_time_change("alter table %(TBL)s drop partition (j=1, s='2012')")
    h.expect_no_time_change("alter table %(TBL)s drop if exists "
                            "partition (j=2, s='2012')")
    # change location of table
    h.expect_ddl_time_change("alter table %(TBL)s set location '%(WAREHOUSE)s'")
    # change format of table
    h.expect_ddl_time_change("alter table %(TBL)s set fileformat textfile")

    self.run_common_test_cases(h)

    # prepare for incremental statistics tests
    h.expect_stat_time_unset("drop stats %(TBL)s")
    self.execute_query("alter table %s add partition (j=1, s='2012')" % h.fq_tbl_name)

    # compute incremental statistics
    h.expect_stat_time_set("compute incremental stats %(TBL)s")
    h.expect_stat_time_change("compute incremental stats %(TBL)s "
                              "partition (j=1, s='2012')")

    # drop incremental statistics
    h.expect_no_time_change("drop incremental stats %(TBL)s partition (j=1, s='2012')")

    # prepare for table sample statistics tests
    self.execute_query(
        "alter table %s set tblproperties ('impala.enable.stats.extrapolation'='true')"
        % h.fq_tbl_name)

    # compute sampled statistics
    h.expect_stat_time_change("compute stats %(TBL)s tablesample system(20)")

  def test_hdfs_insert(self, vector, unique_database):
    TBL_NAME = "insert_test_tbl"
    h = self._create_and_init_test_helper(
        unique_database, TBL_NAME, self.TableFormat.HDFS)

    # static partition insert
    h.expect_no_time_change("insert into %(TBL)s partition(j=1, s='2012') select 10")
    # dynamic partition insert
    h.expect_no_time_change("insert into %(TBL)s partition(j, s) select 10, 2, '2013'")

    # dynamic partition insert changing no partitions (empty input)
    h.expect_no_time_change("insert into %(TBL)s partition(j, s) "
                            "select * from (select 10 as i, 2 as j, '2013' as s) as t "
                            "where t.i < 10")
    # dynamic partition insert modifying an existing partition
    h.expect_no_time_change("insert into %(TBL)s partition(j, s) select 20, 1, '2012'")

  def test_kudu_alter_and_insert(self, vector, unique_database):
    TBL_NAME = "kudu_test_tbl"
    h = self._create_and_init_test_helper(
        unique_database, TBL_NAME, self.TableFormat.KUDU)

    # insert
    h.expect_no_time_change("insert into %s values (1)" % h.fq_tbl_name)

    self.run_common_test_cases(h)

  def test_iceberg_alter_and_insert(self, vector, unique_database):
    TBL_NAMES = ("iceberg_test_tbl", "non_integrated_iceberg_test_tbl")
    helpers = (self._create_and_init_test_helper(
                unique_database, TBL_NAMES[0], self.TableFormat.INTEGRATED_ICEBERG),
              self._create_and_init_test_helper(
                unique_database, TBL_NAMES[1], self.TableFormat.NON_INTEGRATED_ICEBERG))

    for h in helpers:
      # insert
      h.expect_no_time_change("insert into %(TBL)s select 10, 2, '2025'")
      h.expect_no_time_change("insert into %(TBL)s select 20, 1, '2012'")

      # add, alter and drop column
      h.expect_ddl_time_change("alter table %(TBL)s add column a boolean")
      h.expect_ddl_time_change("alter table %(TBL)s alter column a set comment 'bool'")
      h.expect_ddl_time_change("alter table %(TBL)s drop column a")

      # drop partition
      h.expect_ddl_time_change("alter table %(TBL)s drop partition (j=1, s='2012')")

      # set partition spec
      h.expect_ddl_time_change("alter table %(TBL)s set partition spec (void(j))")

      # expire snapshots
      h.expect_ddl_time_change("alter table %(TBL)s execute expire_snapshots(now())")

      self.run_common_test_cases(h)

  def test_rename(self, vector, unique_database):
    # Test HDFS table
    OLD_HDFS_TBL_NAME = "hdfs_rename_from_test_tbl"
    NEW_HDFS_TBL_NAME = "hdfs_rename_to_test_tbl"

    h = self._create_and_init_test_helper(
        unique_database, OLD_HDFS_TBL_NAME, self.TableFormat.HDFS)
    h.expect_ddl_time_change_on_rename(NEW_HDFS_TBL_NAME, True)

    # Test Kudu table
    OLD_KUDU_TBL_NAME = "kudu_rename_from_test_tbl"
    NEW_KUDU_TBL_NAME = "kudu_rename_to_test_tbl"
    h = self._create_and_init_test_helper(
        unique_database, OLD_KUDU_TBL_NAME, self.TableFormat.KUDU)
    h.expect_ddl_time_change_on_rename(NEW_KUDU_TBL_NAME, True)

    # The name of an Iceberg table is not tracked by Iceberg in the metadata files.
    # Table name is a catalog-level abstraction, therefore rename is a catalog-level
    # operation which does not change 'transient_lastDdlTime' table property.
    # Iceberg tables that use 'hadoop.tables' as catalog cannot be renamed.
    OLD_ICEBERG_TBL_NAME = "iceberg_rename_from_test_tbl"
    NEW_ICEBERG_TBL_NAME = "iceberg_rename_to_test_tbl"
    h = self._create_and_init_test_helper(
        unique_database, OLD_ICEBERG_TBL_NAME, self.TableFormat.INTEGRATED_ICEBERG)
    h.expect_ddl_time_change_on_rename(NEW_ICEBERG_TBL_NAME, False)

  # Tests that should behave the same with HDFS, Kudu and Iceberg tables.
  def run_common_test_cases(self, test_helper):
    h = test_helper
    # rename columns
    h.expect_ddl_time_change("alter table %(TBL)s change column i k int")
    h.expect_ddl_time_change("alter table %(TBL)s change column k i int")
    # change table property
    h.expect_ddl_time_change("alter table %(TBL)s set tblproperties ('a'='b')")
    h.expect_ddl_time_change("alter table %(TBL)s unset tblproperties ('a')")

    # changing table statistics manually
    h.expect_ddl_time_change("alter table %(TBL)s set tblproperties ('numRows'='1')")

    # changing column statistics manually
    h.expect_no_time_change("alter table %(TBL)s set column stats i ('numdvs'='3')")

    # compute statistics
    h.expect_stat_time_change("compute stats %(TBL)s")
    # compute statistics for a single column
    h.expect_stat_time_change("compute stats %(TBL)s (i)")

    # drop statistics
    h.expect_stat_time_unset("drop stats %(TBL)s")
    self.execute_query("compute stats  %s" % h.fq_tbl_name)

    # invalidate metadata and reload table
    self.execute_query("invalidate metadata %s" % h.fq_tbl_name)
    # run any query to reload the table
    h.expect_no_time_change("describe %(TBL)s")
