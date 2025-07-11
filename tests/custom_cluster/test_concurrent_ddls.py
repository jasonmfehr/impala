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
from builtins import range
import pytest
import re
import threading
import time

from multiprocessing.pool import ThreadPool
from multiprocessing import TimeoutError

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_connection import (
    ERROR, FINISHED, IMPALA_CONNECTION_EXCEPTION)
from tests.util.shell_util import dump_server_stacktraces


class TestConcurrentDdls(CustomClusterTestSuite):
  """Test concurrent DDLs with invalidate metadata
     TODO: optimize the time dropping the unique_database at the end which dominants
     test time. It currently takes >1m. Most of the time spent in HMS."""

  def _make_per_impalad_args(local_catalog_enabled):
    assert isinstance(local_catalog_enabled, list)
    args = ['--use_local_catalog=%s' % str(e).lower()
            for e in local_catalog_enabled]
    return "--per_impalad_args=" + ";".join(args)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full")
  def test_ddls_with_invalidate_metadata(self, unique_database):
    self._run_ddls_with_invalidation(unique_database, sync_ddl=False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --max_wait_time_for_sync_ddl_s=10")
  def test_ddls_with_invalidate_metadata_sync_ddl(self, unique_database):
    self._run_ddls_with_invalidation(unique_database, sync_ddl=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    start_args=_make_per_impalad_args([True, False]),
    catalogd_args="--catalog_topic_mode=mixed")
  def test_mixed_catalog_ddls_with_invalidate_metadata(self, unique_database):
    self._run_ddls_with_invalidation(unique_database, sync_ddl=False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    start_args=_make_per_impalad_args([True, False]),
    catalogd_args="--catalog_topic_mode=mixed --max_wait_time_for_sync_ddl_s=10")
  def test_mixed_catalog_ddls_with_invalidate_metadata_sync_ddl(self, unique_database):
    self._run_ddls_with_invalidation(unique_database, sync_ddl=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal")
  def test_local_catalog_ddls_with_invalidate_metadata(self, unique_database):
    self._run_ddls_with_invalidation(unique_database, sync_ddl=False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --max_wait_time_for_sync_ddl_s=10")
  def test_local_catalog_ddls_with_invalidate_metadata_sync_ddl(self, unique_database):
    self._run_ddls_with_invalidation(unique_database, sync_ddl=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal "
                  "--reset_metadata_lock_duration_ms=50 "
                  "--debug_actions=reset_metadata_loop_unlocked:SLEEP@50")
  def test_local_catalog_ddls_with_invalidate_metadata_unlock_gap(self, unique_database):
    """Test with 50ms write unlock gap."""
    self._run_ddls_with_invalidation(unique_database, sync_ddl=False)

  def _run_ddls_with_invalidation(self, db, sync_ddl=False):
    """Test INVALIDATE METADATA with concurrent DDLs to see if any queries hang"""
    test_self = self

    class ThreadLocalClient(threading.local):
      def __init__(self):
        self.client = test_self.create_impala_client()
        if sync_ddl:
          self.client.set_configuration_option('sync_ddl', 'true')

    pool = ThreadPool(processes=8)
    tls = ThreadLocalClient()

    def run_ddls(i):
      # Add a sleep so global INVALIDATE has more chance to run concurrently with other
      # DDLs.
      time.sleep(i % 5)
      tbl_name = db + ".test_" + str(i)
      # func_name = "f_" + str(i)
      for query in [
        # alter database operations
        # TODO (IMPALA-9532): Uncomment the alter database operations
        # "comment on database %s is 'test-concurrent-ddls'" % db,
        # "alter database %s set owner user `test-user`" % db,
        # "create function %s.%s() returns int location '%s/libTestUdfs.so' \
        #    symbol='NoArgs'" % (db, func_name, WAREHOUSE),
        # "drop function if exists %s.%s()" % (db, func_name),
        # Create a partitioned and unpartitioned table
        "create table %s (i int)" % tbl_name,
        "create table %s_part (i int) partitioned by (j int)" % tbl_name,
        # Below queries could fail if running with invalidate metadata concurrently
        "alter table %s_part add partition (j=1)" % tbl_name,
        "alter table %s_part add partition (j=2)" % tbl_name,
        "alter table {0} rename to {0}_2".format(tbl_name),
        "alter table {0}_part rename to {0}_part2".format(tbl_name),
        "alter table {0}_2 rename to {0}".format(tbl_name),
        "alter table {0}_part2 rename to {0}_part".format(tbl_name),
        "invalidate metadata %s_part" % tbl_name,
        "refresh %s" % tbl_name,
        "refresh %s_part" % tbl_name,
        "insert overwrite table %s select int_col from "
        "functional.alltypestiny" % tbl_name,
        "insert overwrite table %s_part partition(j=1) "
        "values (1), (2), (3), (4), (5)" % tbl_name,
        "insert overwrite table %s_part partition(j=2) "
        "values (1), (2), (3), (4), (5)" % tbl_name
      ]:
        # Running concurrent with INVALIDATE METADATA can raise an exception. These are
        # safe to retry, so do that until we get a success.
        while True:
          try:
            handle = tls.client.execute_async(query)
            is_finished = tls.client.wait_for_finished_timeout(handle, timeout=120)
            assert is_finished, "Query timeout(120s): " + query
            tls.client.close_query(handle)
            # Success, next case.
            break
          except IMPALA_CONNECTION_EXCEPTION as e:
            err = str(e)
            if self.is_transient_error(err):
              # Retry the query.
              continue
            assert self.is_acceptable_error(err, sync_ddl), err
      self.execute_query_expect_success(tls.client, "invalidate metadata")
      return True

    # Run DDLs in single thread first. Some bugs causing DDL hangs can be hidden when run
    # with concurrent DDLs.
    res = pool.apply_async(run_ddls, (0,))
    try:
      res.get(timeout=100)
    except TimeoutError:
      dump_server_stacktraces()
      assert False, "Single thread execution timeout!"

    # Run DDLs with invalidate metadata in parallel
    NUM_ITERS = 16
    worker = [None] * (NUM_ITERS + 1)
    for i in range(1, NUM_ITERS + 1):
      worker[i] = pool.apply_async(run_ddls, (i,))
    for i in range(1, NUM_ITERS + 1):
      try:
        worker[i].get(timeout=100)
      except TimeoutError:
        dump_server_stacktraces()
        assert False, "Timeout in thread run_ddls(%d)" % i

  @classmethod
  def is_transient_error(cls, err):
    # DDL/DMLs may fail if running with invalidate metadata concurrently, since in-flight
    # table loadings can't finish if the target table is changed (e.g. reset to unloaded
    # state). See more in CatalogOpExecutor.getExistingTable().
    if "CatalogException: Table" in err and \
        "was modified while operation was in progress, aborting execution" in err:
      return True
    return False

  @classmethod
  def is_acceptable_error(cls, err, sync_ddl):
    # TODO: Consider remove this case after IMPALA-9135 is fixed.
    if sync_ddl:
      if "Couldn't retrieve the catalog topic version for the SYNC_DDL operation" in err\
        and ("The operation has been successfully executed but its effects may have not "
             "been broadcast to all the coordinators.") in err:
        return True
    return False

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal")
  def test_concurrent_invalidate_metadata(self):
    """Test concurrent requests for INVALIDATE METADATA not hang"""
    test_self = self

    class ThreadLocalClient(threading.local):
      def __init__(self):
        self.client = test_self.create_impala_client()

    tls = ThreadLocalClient()

    def run_invalidate_metadata():
      # TODO(IMPALA-9123): Detect hangs here instead of using pytest.mark.timeout
      self.execute_query_expect_success(tls.client, "invalidate metadata")

    NUM_ITERS = 20
    pool = ThreadPool(processes=2)
    for i in range(NUM_ITERS):
      # Run two INVALIDATE METADATA commands in parallel
      r1 = pool.apply_async(run_invalidate_metadata)
      r2 = pool.apply_async(run_invalidate_metadata)
      try:
        r1.get(timeout=60)
        r2.get(timeout=60)
      except TimeoutError:
        dump_server_stacktraces()
        assert False, "INVALIDATE METADATA timeout in 60s!"
    pool.terminate()

  @CustomClusterTestSuite.with_args(
    catalogd_args="--enable_incremental_metadata_updates=true")
  def test_concurrent_invalidate_metadata_with_refresh(self, unique_database):
    # Create a wide table with some partitions
    tbl = unique_database + ".wide_tbl"
    create_stmt = "create table {} (".format(tbl)
    for i in range(600):
      create_stmt += "col{} int, ".format(i)
    create_stmt += "col600 int) partitioned by (p int) stored as textfile"
    self.execute_query(create_stmt)
    for i in range(10):
      self.execute_query("alter table {} add partition (p={})".format(tbl, i))

    refresh_stmt = "refresh " + tbl
    refresh_handle = self.client.execute_async(refresh_stmt)
    for i in range(10):
      self.execute_query("invalidate metadata " + tbl)
      # Always keep a concurrent REFRESH statement running
      refresh_state = self.client.get_impala_exec_state(refresh_handle)
      if refresh_state == FINISHED or ERROR:
        refresh_handle = self.client.execute_async(refresh_stmt)
