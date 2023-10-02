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

import os
import re
import requests
import string
import tempfile

from datetime import datetime
from random import choice, randint
from signal import SIGRTMIN
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.memory import assert_byte_str, convert_to_bytes
from tests.util.retry import retry
from tests.util.assert_time import assert_time_str, convert_to_nanos
from time import sleep, time

# Tests to add:
#   * more iterations of test_query_log_table_fields_beeswax with failed queries
#   * exceed startup flag limitations


class TestQueryLogTable(CustomClusterTestSuite):
  """Tests to assert the query log table is correctly populated."""

  QUERY_TBL = "sys.impala_query_log"
  EXPECTED_QUERY_COLS = 48

  def assert_query(self, client, expected_cluster_id, raw_profile=None, impalad=None,
                   query_id=None):
    """Helper function to assert that the values in the completed query log table
       match the values from the query profile."""

    # If query_id was specified, read the profile from the Impala webserver.
    if query_id is not None:
      assert impalad is not None
      assert raw_profile is None, "cannot specify both query_id and raw_profile"
      resp = requests.get("http://{0}:{1}/query_profile_plain_text?query_id={2}"
                        .format(impalad.hostname, impalad.get_webserver_port(), query_id))
      assert resp.status_code == 200, "Response code was: {0}".format(resp.status_code)
      profile_text = resp.text
    else:
      profile_text = raw_profile
      assert query_id is None, "cannot specify both raw_profile and query_id"
      match = re.search(r'Query \(id=(.*?)\)', profile_text)
      assert match is not None
      query_id = match.group(1)

    print("Query Id: {0}".format(query_id))
    profile_lines = profile_text.split("\n")

    # Assert the query was written correctly to the query log table.
    assert_sql = "select * from {0} where query_id='{1}'" \
                  .format(self.QUERY_TBL, query_id)
    sql_results = client.execute(assert_sql)
    assert sql_results.success

    # Assert the expected columns were included.
    assert len(sql_results.data) == 1
    assert len(sql_results.column_labels) == self.EXPECTED_QUERY_COLS
    data = sql_results.data[0].split("\t")
    assert len(data) == len(sql_results.column_labels)

    # Cluster ID
    index = 0
    assert sql_results.column_labels[index] == "CLUSTER_ID"
    assert data[index] == expected_cluster_id, "cluster id incorrect"

    # Query ID
    index += 1
    assert sql_results.column_labels[index] == "QUERY_ID"
    assert data[index] == query_id

    # Session ID
    index += 1
    assert sql_results.column_labels[index] == "SESSION_ID"
    session_id = re.search(r'\n\s+Session ID:\s+(.*)', profile_text)
    assert session_id is not None
    assert data[index] == session_id.group(1)

    # Session Type
    index += 1
    assert sql_results.column_labels[index] == "SESSION_TYPE"
    session_type = re.search(r'\n\s+Session Type:\s+(.*)', profile_text)
    assert session_type is not None
    assert data[index] == session_type.group(1)

    # HS2 Protocol Version
    index += 1
    assert sql_results.column_labels[index] == "HIVESERVER2_PROTOCOL_VERSION"
    if session_type == "HIVESERVER2":
      hs2_ver = re.search(r'\n\s+HiveServer2 Protocol Version:\s+(.*)', profile_text)
      assert hs2_ver is not None
      assert data[index] == hs2_ver.group(1)
    else:
      assert data[index] == ""

    # Database User
    index += 1
    assert sql_results.column_labels[index] == "DB_USER"
    user = re.search(r'\n\s+User:\s+(.*?)\n', profile_text)
    assert user is not None
    assert data[index] == user.group(1)

    # Connected Database User
    index += 1
    assert sql_results.column_labels[index] == "DB_USER_CONNECTION"
    db_user = re.search(r'\n\s+Connected User:\s+(.*?)\n', profile_text)
    assert db_user is not None
    assert data[index] == db_user.group(1)

    # Database Name
    index += 1
    assert sql_results.column_labels[index] == "DB_NAME"
    default_db = re.search(r'\n\s+Default Db:\s+(.*?)\n', profile_text)
    assert default_db is not None
    assert data[index] == default_db.group(1)

    # Coordinator
    index += 1
    assert sql_results.column_labels[index] == "IMPALA_COORDINATOR"
    coordinator = re.search(r'\n\s+Coordinator:\s+(.*?)\n', profile_text)
    assert coordinator is not None
    assert data[index] == coordinator.group(1), "impala coordinator incorrect"

    # Query Status (can be multiple lines if the query errored)
    index += 1
    assert sql_results.column_labels[index] == "QUERY_STATUS"
    found = False
    query_status_text = ""
    for line in profile_lines:
      if not found:
        query_status = re.search(r'^\s+Query Status:\s+(.*?)$', line)
        if query_status is None:
          continue
        query_status_text = query_status.group(1)
        found = True
      else:
        if line.startswith(" "):
          break
        query_status_text += "\n" + line

    assert data[index] == query_status_text, "query status incorrect"

    # Query State
    index += 1
    assert sql_results.column_labels[index] == "QUERY_STATE"
    query_state = re.search(r'\n\s+Query State:\s+(.*?)\n', profile_text)
    assert query_state is not None
    query_state_value = query_state.group(1)
    assert data[index] == query_state_value, "query state incorrect"

    # Impala Query End State
    index += 1
    assert sql_results.column_labels[index] == "IMPALA_QUERY_END_STATE"
    impala_query_state = re.search(r'\n\s+Impala Query State:\s+(.*?)\n', profile_text)
    assert impala_query_state is not None
    assert data[index] == impala_query_state.group(1), \
        "impala query end state incorrect"

    # Query Type
    index += 1
    assert sql_results.column_labels[index] == "QUERY_TYPE"
    if query_state_value == "EXCEPTION":
      assert data[index] == "N/A", "query type incorrect"
    else:
      query_type = re.search(r'\n\s+Query Type:\s+(.*?)\n', profile_text)
      assert query_type is not None
      assert data[index] == query_type.group(1), "query type incorrect"
      query_type = query_type.group(1)

    # Client Network Address
    index += 1
    assert sql_results.column_labels[index] == "NETWORK_ADDRESS"
    network_address = re.search(r'\n\s+Network Address:\s+(.*?)\n', profile_text)
    assert network_address is not None
    assert data[index] == network_address.group(1), "network address incorrect"

    # offset from UTC
    utc_now = datetime.utcnow().replace(microsecond=0, second=0)
    local_now = datetime.now().replace(microsecond=0, second=0)
    utc_offset = utc_now - local_now

    # Start Time
    index += 1
    assert sql_results.column_labels[index] == "START_TIME_UTC"
    start_time = re.search(r'\n\s+Start Time:\s+(.*?)\n', profile_text)
    assert start_time is not None
    start_time_obj = datetime.strptime(start_time.group(1)[:-3], "%Y-%m-%d %H:%M:%S.%f")
    start_time_obj_utc = start_time_obj + utc_offset
    assert data[index][:-3] == start_time_obj_utc.strftime("%Y-%m-%d %H:%M:%S.%f"), \
        "start time incorrect"

    # End Time (not in table, but needed for duration calculation)
    end_time = re.search(r'\n\s+End Time:\s+(.*?)\n', profile_text)
    assert end_time is not None
    end_time_obj = datetime.strptime(end_time.group(1)[:-3], "%Y-%m-%d %H:%M:%S.%f")

    # Query Duration (allow values that are within 1 second)
    index += 1
    assert sql_results.column_labels[index] == "TOTAL_TIME_NS"
    duration = end_time_obj - start_time_obj
    min_allowed = int(duration.total_seconds() * 1000000000) - 1
    max_allowed = min_allowed + 2
    assert min_allowed <= int(data[index]) <= max_allowed, "total time incorrect"

    # SQL statement
    index += 1
    assert sql_results.column_labels[index] == "SQL"
    sql_stmt = re.search(r'\n\s+Sql Statement:\s+(.*?)\n', profile_text)
    assert sql_stmt is not None
    assert data[index] == sql_stmt.group(1), "sql incorrect"

    # Query Options Set By Configuration
    # TODO - test actual query options
    index += 1
    assert sql_results.column_labels[index] == "QUERY_OPTS_CONFIG"
    if query_state_value == "EXCEPTION":
      assert data[index] != "", "query options set by config incorrect"
    else:
      sql_stmt = re.search(r'\n\s+Query Options \(set by configuration\):\s+(.*?)\n',
                          profile_text)
      assert sql_stmt is not None
      assert data[index] == sql_stmt.group(1), "query opts set by config incorrect"

    # Resource Pool
    index += 1
    assert sql_results.column_labels[index] == "RESOURCE_POOL"
    if query_state_value == "EXCEPTION":
      assert data[index] == "", "resource pool incorrect"
    else:
      if query_type != "DDL":
        req_pool = re.search(r'\n\s+Request Pool:\s+(.*?)\n', profile_text)
        assert req_pool is not None
        assert data[index] == req_pool.group(1), "request pool incorrect"
      else:
        assert data[index] == "", "request pool not empty"

    # Per-host Memory Estimate
    index += 1
    assert sql_results.column_labels[index] == "PER_HOST_MEM_ESTIMATE"
    if query_state_value == "EXCEPTION":
      assert data[index] == "0", "per-host memory estimate incorrect"
    else:
      if query_type != "DDL":
        perhost_mem_est = re.search(r'\nPer-Host Resource Estimates:\s+Memory\=(.*?)\n',
                                    profile_text)
        assert perhost_mem_est is not None
        profile_est = convert_to_bytes(perhost_mem_est.group(1), True)
        tolerance = int(profile_est * 0.005)
        assert profile_est - tolerance <= int(data[index]) <= profile_est + tolerance, \
            "per-host memory estimate incorrect"
      else:
        assert data[index] == "0", "per-host memory estimate not 0"

    # Dedicated Coordinator Memory Estimate
    # TODO: need to parse profile to get expected value
    index += 1
    assert sql_results.column_labels[index] == "DEDICATED_COORD_MEM_ESTIMATE"

    # Per-Host Fragment Instances
    index += 1
    assert sql_results.column_labels[index] == "PER_HOST_FRAGMENT_INSTANCES"

    # Backends Count
    index += 1
    assert sql_results.column_labels[index] == "BACKENDS_COUNT"
    num_bck = re.search(r'\n\s+\- NumBackends:\s+(\d+)', profile_text)
    if query_state_value == "EXCEPTION" or query_type == "DDL":
      assert num_bck is None
      assert data[index] == "0", "backends count incorrect"
    else:
      assert num_bck is not None
      assert data[index] == num_bck.group(1), "backends count incorrect"

    # Admission Result
    index += 1
    assert sql_results.column_labels[index] == "ADMISSION_RESULT"
    adm_result = re.search(r'\n\s+Admission result:\s+(.*?)\n', profile_text)
    if query_state_value == "EXCEPTION" or query_type == "DDL":
      assert adm_result is None
      assert data[index] == "", "admission result incorrect"
    else:
      assert adm_result is not None
      assert data[index] == adm_result.group(1), "admission result incorrect"

    # Cluster Memory Admitted
    index += 1
    assert sql_results.column_labels[index] == "CLUSTER_MEMORY_ADMITTED"
    clust_mem = re.search(r'\n\s+Cluster Memory Admitted:\s+(.*?)\n', profile_text)
    if query_state_value == "EXCEPTION":
      assert clust_mem is None
    else:
      if query_type != "DDL":
        assert clust_mem is not None
        assert_byte_str(clust_mem.group(1), data[index],
                        "cluster memory admitted incorrect")
      else:
        assert data[index] == "0", "cluster memory not zero"

    # Executor Group
    index += 1
    assert sql_results.column_labels[index] == "EXECUTOR_GROUP"
    exec_group = re.search(r'\n\s+Executor Group:\s+(.*?)\n', profile_text)
    if query_state_value == "EXCEPTION" or query_type == "DDL":
      assert exec_group is None
      assert data[index] == "", "executor group should not have been found"
    else:
      assert exec_group is not None
      assert data[index] == exec_group.group(1), "executor group incorrect"

    # Executor Groups
    index += 1
    assert sql_results.column_labels[index] == "EXECUTOR_GROUPS"
    exec_groups = re.search(r'\n\s+(Executor group \d+:.*?)\n\s+ImpalaServer',
        profile_text, re.DOTALL)
    if query_state_value == "EXCEPTION":
      assert exec_groups is None, "executor groups should not have been found"
    else:
      assert exec_groups is not None
      dedent_str = re.sub(r'^\s{6}', '', exec_groups.group(1), flags=re.MULTILINE)
      print("FROM DB:\n'{0}'".format(data[index]))
      print("FROM PROF:\n'{0}'".format(dedent_str))
      assert data[index] == dedent_str, "executor groups incorrect"

    # Exec Summary
    index += 1
    assert sql_results.column_labels[index] == "EXEC_SUMMARY"
    exec_sum = re.search(r'\n\s+ExecSummary:\s*\n(.*)\n\s+Errors', profile_text,
                         re.DOTALL)
    if query_state_value == "EXCEPTION" or query_type == "DDL":
      assert exec_sum is None
      assert data[index] == ""
    else:
      assert exec_sum is not None
      assert data[index] == exec_sum.group(1)

    # Query Plan
    index += 1
    assert sql_results.column_labels[index] == "PLAN"
    plan = re.search(r'\n\s+Plan:\s*\n(.*)\n\s+Estimated Per-Host Mem', profile_text,
                         re.DOTALL)
    if query_state_value == "EXCEPTION" or query_type == "DDL":
      assert plan is None
      assert data[index] == ""
    else:
      assert plan is not None
      assert data[index] == plan.group(1)

    # Rows Fetched
    index += 1
    assert sql_results.column_labels[index] == "NUM_ROWS_FETCHED"
    rows_fetched = re.search(r'\n\s+\-\s+NumRowsFetched:\s+\S+\s+\((\d+)\)', profile_text)
    if query_state_value == "EXCEPTION":
      assert rows_fetched is None
    else:
      assert rows_fetched is not None
      assert data[index] == rows_fetched.group(1)

    # Row Materialization Rate
    index += 1
    assert sql_results.column_labels[index] == "ROW_MATERIALIZATION_BYTES_PER_SEC"
    row_mat = re.search(r'\n\s+\-\s+RowMaterializationRate:\s+(.*?)\n', profile_text)
    if query_state_value == "EXCEPTION":
      assert row_mat is None
    elif query_type == "DDL" or query_type == 'DML':
      assert row_mat is not None
      assert row_mat.group(1) == "0", "row materialization rate incorrect"
    else:
      assert row_mat is not None
      row_mat_str = row_mat.group(1)[:-4]
      assert_byte_str(row_mat_str, data[index], "row materialization rate incorrect",
                      1000)

    # Row Materialization Time
    index += 1
    assert sql_results.column_labels[index] == "ROW_MATERIALIZATION_TIME_NS"
    row_mat_tmr = re.search(r'\n\s+\-\s+RowMaterializationTimer:\s+(.*?)\n', profile_text)
    if query_state_value == "EXCEPTION":
      assert row_mat_tmr is None
    elif query_type == "DDL" or query_type == 'DML':
      assert row_mat_tmr is not None
      assert row_mat_tmr.group(1) == "0.000ns", "row materialization timer incorrect"
    else:
      assert row_mat_tmr is not None
      assert_time_str(row_mat_tmr.group(1), (int(data[index])),
                      "row materialization timer incorrect", 2000)

    # Compressed Bytes Spilled
    # TODO - need to test with a query that spills
    index += 1
    assert sql_results.column_labels[index] == "COMPRESSED_BYTES_SPILLED"
    scratch_bytes_total = 0
    for line in profile_lines:
      sbw = re.search(r'^\s+\-\s+ScratchBytesWritten:.*?\((\d+)\)$', line)
      if sbw is not None:
        scratch_bytes_total += int(sbw.group(1))
    assert int(data[index]) == scratch_bytes_total

    # Event Timeline Planning Finished
    index += 1
    assert sql_results.column_labels[index] == "EVENT_PLANNING_FINISHED"

    # Event Timeline Submit for Admission
    index += 1
    assert sql_results.column_labels[index] == "EVENT_SUBMIT_FOR_ADMISSION"

    # Event Timeline Completed Admission
    index += 1
    assert sql_results.column_labels[index] == "EVENT_COMPLETED_ADMISSION"

    # Event Timeline All Backends Started
    index += 1
    assert sql_results.column_labels[index] == "EVENT_ALL_BACKENDS_STARTED"

    # Event Timeline Rows Available
    index += 1
    assert sql_results.column_labels[index] == "EVENT_ROWS_AVAILABLE"

    # Event Timeline First Row Fetched
    index += 1
    assert sql_results.column_labels[index] == "EVENT_FIRST_ROW_FETCHED"

    # Event Timeline Last Row Fetched
    index += 1
    assert sql_results.column_labels[index] == "EVENT_LAST_ROW_FETCHED"

    # Event Timeline Unregister Query
    index += 1
    assert sql_results.column_labels[index] == "EVENT_UNREGISTER_QUERY"

    # The scanner io wait time metric is reported in milliseconds with up to three digits
    # of microsecond precision. However, the read io wait times are reported in
    # nanoseconds in the completed queries table. Thus, this offset gives 5 microseconds
    # of leeway when asserting the values reported in the database are correct.
    offset = int(5000)

    # Read IO Wait Total
    index += 1
    assert sql_results.column_labels[index] == "READ_IO_WAIT_TOTAL_NS"
    total_read_wait = 0
    if (query_state_value != "EXCEPTION" and query_type == "QUERY") or data[index] != "0":
      re_wait_time = re.compile(r'^\s+\-\s+ScannerIoWaitTime:\s+(.*?)$')
      read_waits = assert_scan_node_metrics(re_wait_time, profile_lines)
      for r in read_waits:
        total_read_wait += int(convert_to_nanos(r))

      assert total_read_wait - offset <= int(data[index]) <= total_read_wait + offset, \
          "read io wait time total incorrect"
    else:
      assert data[index] == "0"

    # Read IO Wait Average
    index += 1
    assert sql_results.column_labels[index] == "READ_IO_WAIT_MEAN_NS"
    if (query_state_value != "EXCEPTION" and query_type == "QUERY") or data[index] != "0":
      avg_read_wait = int(total_read_wait / len(read_waits))
      assert avg_read_wait - offset <= int(data[index]) <= avg_read_wait + offset, \
          "read io wait time average incorrect"
    else:
      assert data[index] == "0"

    # Total Bytes Read From Cache
    index += 1
    assert sql_results.column_labels[index] == "BYTES_READ_CACHE_TOTAL"
    if (query_state_value != "EXCEPTION" and query_type == "QUERY") or data[index] != "0":
      re_cache_read = re.compile(r'^\s+\-\s+DataCacheHitBytes:\s+.*?\((\d+)\)$')
      read_from_cache = assert_scan_node_metrics(re_cache_read, profile_lines)

      total_read = 0
      for r in read_from_cache:
        total_read += int(r)
      assert total_read == int(data[index]), "bytes read from cache total incorrect"
    else:
      assert data[index] == "0"

    # Total Bytes Read
    index += 1
    assert sql_results.column_labels[index] == "BYTES_READ_TOTAL"
    total_bytes_read = re.search(r'\n\s+\-\s+TotalBytesRead:\s+.*?\((\d+)\)\n',
        profile_text)
    if query_state_value != "EXCEPTION" and query_type == "QUERY":
      assert total_bytes_read is not None, "total bytes read missing"
    if total_bytes_read is not None:
      assert data[index] == total_bytes_read.group(1), "total bytes read incorrect"

    # Calculate all peak memory usage stats by scraping the query profile.
    peak_mem = re.compile(r'^\s+Per Node Peak Memory Usage:\s+')
    peak_mem_cnt = 0
    min_peak_mem = 0
    max_peak_mem = 0
    total_peak_mem = 0
    for line in profile_lines:
      res = peak_mem.match(line)
      if res is not None:
        for node in re.finditer(r'\s.*?:\d+\((.*?)\)', line):
          peak_mem_cnt += 1
          conv = convert_to_bytes(node.group(1))
          total_peak_mem += conv
          if conv < min_peak_mem or min_peak_mem == 0: min_peak_mem = conv
          if conv > max_peak_mem: max_peak_mem = conv
    if query_state_value != "EXCEPTION" and query_type != "DDL":
      assert peak_mem_cnt > 0, "did not find per node peak memory usage"

    # Per Node Peak Memory Usage Min
    index += 1
    assert sql_results.column_labels[index] == "PERNODE_PEAK_MEM_MIN"
    tolerance = int(min_peak_mem * 0.001)
    assert min_peak_mem - tolerance <= int(data[index]) <= min_peak_mem + tolerance, \
        "pernode peak memory minimum incorrect"

    # Per Node Peak Memory Usage Max
    index += 1
    assert sql_results.column_labels[index] == "PERNODE_PEAK_MEM_MAX"
    tolerance = int(max_peak_mem * 0.003)
    assert max_peak_mem - tolerance <= int(data[index]) <= max_peak_mem + tolerance, \
        "pernode peak memory maximum incorrect"

    # Per Node Peak Memory Usage Mean
    index += 1
    assert sql_results.column_labels[index] == "PERNODE_PEAK_MEM_MEAN"
    mean_peak_mem = 0
    if peak_mem_cnt > 0:
      mean_peak_mem = int(total_peak_mem / peak_mem_cnt)
    tolerance = int(max_peak_mem * 0.001)
    assert mean_peak_mem - tolerance <= int(data[index]) <= mean_peak_mem + tolerance, \
        "pernode peak memory mean incorrect"

    return data
  # function assert_query

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_2 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_fields_beeswax_ddl(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile."""
    tbl_name = "default.test_query_log_beeswax_ddl_" + str(int(time()))
    create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(tbl_name)
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      res = client.execute(create_tbl_sql)
      assert res.success
      impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 1,
          60)

      client2 = self.create_client_for_nth_impalad(2)
      assert client2 is not None
      self.assert_query(client2, "test_query_hist_2", res.runtime_profile)
    finally:
      client.execute("drop table if exists {0}".format(tbl_name))
      client.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_3 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_fields_beeswax_dml(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile."""
    tbl_name = "default.test_query_log_beeswax_dml_" + str(int(time()))
    create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(tbl_name)
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      # Create the test table.
      create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(tbl_name)
      create_tbl_results = client.execute(create_tbl_sql)
      assert create_tbl_results.success

      insert_sql = "insert into {0} (id,category,product_name) values " \
                   "(0,1,'the product')".format(tbl_name)
      res = client.execute(insert_sql)
      assert res.success
      impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 2,
          60)

      client2 = self.create_client_for_nth_impalad(2)
      assert client2 is not None
      self.assert_query(client2, "test_query_hist_3", res.runtime_profile)
    finally:
      client.execute("drop table if exists {0}".format(tbl_name))
      client.close()

  # TODO: test with hs2 protocol
  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_1 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_fields_beeswax_query_select(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile."""
    tbl_name = "default.test_query_log_beeswax_" + str(int(time()))
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      # Create the test table.
      create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(tbl_name)
      create_tbl_results = client.execute(create_tbl_sql)
      assert create_tbl_results.success

      # Insert some rows into the test table.
      insert_sql = "insert into {0} (id,category,product_name) VALUES ".format(tbl_name)
      for i in range(1, 11):
        for j in range(1, 11):
          if i * j > 1:
            insert_sql += ","

          random_product_name = "".join(choice(string.ascii_letters)
            for _ in range(10))
          insert_sql += "({0},{1},'{2}')".format((i * j), i, random_product_name)

      insert_results = client.execute(insert_sql)
      assert insert_results.success

      # Select all rows from the test table.
      random_sleep = randint(1, 500)
      select_sql = "select * from {0} where id != sleep({1})" \
          .format(tbl_name, random_sleep)
      client.set_configuration_option("MAX_MEM_ESTIMATE_FOR_ADMISSION", "10MB")
      res = client.execute(select_sql)
      assert res.success
      impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 3,
          60)

      client2 = self.create_client_for_nth_impalad(2)
      assert client2 is not None
      self.assert_query(client2, "test_query_hist_1", res.runtime_profile)
    finally:
      client.execute("drop table if exists {0}".format(tbl_name))
      client.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_mult "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60",
                                    catalogd_args="--enable_workload_mgmt",
                                    cluster_size=2,
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_fields_beeswax_query_multiple(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile for a query that reads from multiple tables."""
    tbl_name = "default.test_query_log_beeswax_" + str(int(time()))
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      # Create the first test table.
      create_tbl_sql = "create table {0}_products (id INT, product_name STRING)" \
          .format(tbl_name)
      create_tbl_results = client.execute(create_tbl_sql)
      assert create_tbl_results.success

      # Insert some rows into the test products table.
      insert_sql = "insert into {0}_products (id,product_name) VALUES ".format(tbl_name)
      for i in range(1, 11):
        for j in range(1, 11):
          if i * j > 1:
            insert_sql += ","

          random_product_name = "".join(choice(string.ascii_letters) for _ in range(10))
          insert_sql += "({0},'{1}')".format((i * j), random_product_name)

      insert_results = client.execute(insert_sql)
      assert insert_results.success

      # Create the second test table.
      create_tbl_sql = "create table {0}_customers (id INT, name STRING) " \
          .format(tbl_name)
      create_tbl_results = client.execute(create_tbl_sql)
      assert create_tbl_results.success

      # Insert rows into the test customers table.
      insert_sql = "insert into {0}_customers (id,name) VALUES ".format(tbl_name)
      for i in range(1, 11):
        if i > 1:
          insert_sql += ","
        rand_cust_name = "".join(choice(string.ascii_letters) for _ in range(10))
        insert_sql += "({0},'{1}')".format(i, rand_cust_name)

      insert_results = client.execute(insert_sql)
      assert insert_results.success

      # Create the third test table.
      create_tbl_sql = "create table {0}_sales (id INT, product_id INT, " \
          "customer_id INT) ".format(tbl_name)
      create_tbl_results = client.execute(create_tbl_sql)
      assert create_tbl_results.success

      # Insert rows into the test sales table.
      insert_sql = "insert into {0}_sales (id, product_id, customer_id) VALUES " \
          .format(tbl_name)
      for i in range(1, 1001):
        if i != 1:
          insert_sql += ","
        insert_sql += "({0},{1},{2})".format(i * j, randint(1, 100), randint(1, 10))

      insert_results = client.execute(insert_sql)
      assert insert_results.success

      # Select all rows from the test table.
      client.set_configuration_option("MAX_MEM_ESTIMATE_FOR_ADMISSION", "10MB")
      res = client.execute("select s.id, p.product_name, c.name from {0}_sales s "
          "inner join {0}_products p on s.product_id=p.id "
          "inner join {0}_customers c on s.customer_id=c.id".format(tbl_name))
      assert res.success
      impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 7,
          60)

      client2 = self.create_client_for_nth_impalad(1)
      assert client2 is not None
      self.assert_query(client2, "test_query_hist_mult", res.runtime_profile)
    finally:
      client.execute("drop table if exists {0}_sales".format(tbl_name))
      client.execute("drop table if exists {0}_customers".format(tbl_name))
      client.execute("drop table if exists {0}_products".format(tbl_name))
      client.close()

  CACHE_DIR = tempfile.mkdtemp(prefix="cache_dir")

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_2 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60 "
                                                 "--always_use_data_cache "
                                                 "--data_cache={0}:5GB".format(CACHE_DIR),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True,
                                    cluster_size=1)
  def test_query_log_table_fields_beeswax_query_cache(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile. Specifically focuses on the data cache metrics."""
    tbl_name = "default.test_query_log_beeswax_cache" + str(int(time()))
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      # Create the test table.
      create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(tbl_name)
      create_tbl_results = client.execute(create_tbl_sql)
      assert create_tbl_results.success

      # Insert some rows into the test table.
      insert_sql = "insert into {0} (id,category,product_name) VALUES ".format(tbl_name)
      for i in range(1, 11):
        for j in range(1, 11):
          if i * j > 1:
            insert_sql += ","

          random_product_name = "".join(choice(string.ascii_letters)
            for _ in range(10))
          insert_sql += "({0},{1},'{2}')".format((i * j), i, random_product_name)

      insert_results = client.execute(insert_sql)
      assert insert_results.success

      # Select all rows from the test table.
      select_sql = "select * from {0}".format(tbl_name)
      res = client.execute(select_sql)
      assert res.success
      impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 3,
          60)

      # Run the same query again so results are read from the data cache.
      res = client.execute(select_sql)
      assert res.success
      impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 4,
          60)

      data = self.assert_query(client, "test_query_hist_2", res.runtime_profile)
      assert data[42] != "0", "bytes read from cache total was zero, " \
          "test did not assert anything"
    finally:
      client.execute("drop table if exists {0}".format(tbl_name))
      client.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_3 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_fields_beeswax_query_insert_select(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile for a query that insert selects."""
    tbl_name = "default.test_query_log_beeswax_insert_select" + str(int(time()))
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      # Create the source test table.
      assert client.execute("create table {0}_source (id INT, product_name STRING) "
          .format(tbl_name)).success, "could not create source table"

      # Insert some rows into the test table.
      insert_sql = "insert into {0}_source (id,product_name) VALUES " \
          .format(tbl_name)
      for i in range(1, 100):
        if i > 1:
          insert_sql += ","

        random_product_name = "".join(choice(string.ascii_letters)
          for _ in range(10))
        insert_sql += "({0},'{1}')".format(i, random_product_name)

      assert client.execute(insert_sql).success, "could not insert rows"

      # Create the destination test table.
      assert client.execute("create table {0}_dest (id INT, product_name STRING) "
          .format(tbl_name)).success, "could not create destination table"

      # Insert select from the source table to the destination table.
      res = client.execute("insert into {0}_dest (id, product_name) select id, "
          "product_name from {0}_source".format(tbl_name))
      assert res.success, "could not insert select"

      impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 4,
          60)

      client2 = self.create_client_for_nth_impalad(2)
      assert client2 is not None
      self.assert_query(client2, "test_query_hist_3", res.runtime_profile)
    finally:
      client.execute("drop table if exists {0}_source".format(tbl_name))
      client.execute("drop table if exists {0}_dest".format(tbl_name))
      client.close()

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--cluster_id=test_query_hist_2 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60 ",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_beeswax_invalid_query(self, vector):
    """Asserts correct values are written to the completed queries table for a failed
       query. The query profile is used as the source of expected values."""
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    # Assert an invalid query
    unix_now = time()
    try:
      client.execute("{0}".format(unix_now))
    except Exception as _:
      pass

    # Get the query id from the completed queries table since the call to execute errors
    # instead of return the results object which contains the query id.
    impalad.service.wait_for_metric_value("impala-server.completed_queries.written", 1,
        60)
    result = client.execute("select query_id from {0} where sql='{1}'"
                            .format(self.QUERY_TBL, unix_now))
    assert result.success
    assert len(result.data) == 1

    self.assert_query(client=client, expected_cluster_id="test_query_hist_2",
                      impalad=impalad, query_id=result.data[0])

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_ignored_sqls(self, vector):
    """Asserts that expected queries are not written to the query log table."""
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    sqls = {}
    sqls["use default"] = True
    sqls["USE default"] = True
    sqls["uSe default"] = True
    sqls["--mycomment\nuse default"] = True
    sqls["/*mycomment*/ use default"] = True

    sqls["set all"] = True
    sqls["SET all"] = True
    sqls["SeT all"] = True
    sqls["--mycomment\nset all"] = True
    sqls["/*mycomment*/ set all"] = True

    sqls["show tables"] = True
    sqls["SHOW tables"] = True
    sqls["ShoW tables"] = True
    sqls["ShoW create table {0}".format(self.QUERY_TBL)] = True
    sqls["show databases"] = True
    sqls["SHOW databases"] = True
    sqls["ShoW databases"] = True
    sqls["show schemas"] = True
    sqls["SHOW schemas"] = True
    sqls["ShoW schemas"] = True
    sqls["--mycomment\nshow tables"] = True
    sqls["/*mycomment*/ show tables"] = True
    sqls["/*mycomment*/ show tables"] = True
    sqls["/*mycomment*/ show create table {0}".format(self.QUERY_TBL)] = True
    sqls["/*mycomment*/ show files in {0}".format(self.QUERY_TBL)] = True
    sqls["/*mycomment*/ show functions"] = True
    sqls["/*mycomment*/ show data sources"] = True
    sqls["/*mycomment*/ show views"] = True

    sqls["describe database default"] = True
    sqls["/*mycomment*/ describe database default"] = True
    sqls["describe {0}".format(self.QUERY_TBL)] = True
    sqls["/*mycomment*/ describe {0}".format(self.QUERY_TBL)] = True
    sqls["describe history {0}".format(self.QUERY_TBL)] = True
    sqls["/*mycomment*/ describe history {0}".format(self.QUERY_TBL)] = True

    try:
      for sql, should_succeed in sqls.items():
        try:
          results = client.execute(sql)
        except Exception as _:
            if should_succeed:
              assert False, "failed to execute query: {0}".format(sql)
        sqls[sql] = results.query_id

      # Wait for the completed queries daemon to flush to the query log table.
      sleep(5)

      for sql, query_id in sqls.items():
        log_results = client.execute("select * from {0} where query_id='{1}'"
                                     .format(self.QUERY_TBL, query_id))
        assert log_results.success
        assert len(log_results.data) == 0, "found query in query log table: {0}" \
                                               .format(sql)
    finally:
      client.close()

    # Assert there was one query per sql item written to the query log table. The queries
    # inserted into the completed queries table are the queries used to assert the ignored
    # queries were not written to the table.
    impalad.service.wait_for_metric_value(
        "impala-server.completed_queries.written", len(sqls), 60)
    assert impalad.service.get_metric_value("impala-server.completed_queries.failure") \
        == 0

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=1 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_sql_injection(self, vector):
    tbl_name = "default.test_query_log_sql_injection_" + str(int(time()))
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      # Create the test table.
      create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(tbl_name)
      create_tbl_results = client.execute(create_tbl_sql)
      assert create_tbl_results.success

      # Insert some rows into the test table.
      insert_sql = "insert into {0} (id,category,product_name) VALUES ".format(tbl_name)
      for i in range(1, 11):
        for j in range(1, 11):
          if i * j > 1:
            insert_sql += ","

          insert_sql += "({0},{1},'{2}')".format((i * j), i,
                                                  "product-{0}-{1}".format(i, j))

      insert_results = client.execute(insert_sql)
      assert insert_results.success

      # Try a sql injection attack with closing quotes.
      sql1_str = "select * from {0} where product_name='product-2-3'".format(tbl_name)
      self.__run_sql_inject(impalad, client, sql1_str, "closing quotes", 3)

      # Try a sql inject attack with terminating quote and semicolon.
      sql2_str = "select 1'); drop table {0}; select('" \
                 .format(self.QUERY_TBL)
      self.__run_sql_inject(impalad, client, sql2_str, "terminating semicolon", 5)

      # Attempt to cause an error using multiline comments.
      sql3_str = "select 1' /* foo"
      self.__run_sql_inject(impalad, client, sql3_str, "multiline comments", 7, False)

      # Attempt to cause an error using single line comments.
      sql4_str = "select 1' -- foo"
      self.__run_sql_inject(impalad, client, sql4_str, "single line comments", 9, False)

    finally:
      client.execute("drop table if exists {0}".format(tbl_name))
      client.close()

  def __run_sql_inject(self, impalad, client, sql, test_case, expected_writes,
                       expect_success=True):
    sql_result = None
    try:
      sql_result = client.execute(sql)
    except Exception as e:
      if expect_success:
        raise e

    if expect_success:
      assert sql_result.success

    impalad.service.wait_for_metric_value(
        "impala-server.completed_queries.written", expected_writes, 60)

    # Allow time for Impala to process the insert.
    sleep(3)

    if expect_success:
      sql_verify = client.execute(
          "select sql from {0} where query_id='{1}'"
          .format(self.QUERY_TBL, sql_result.query_id))

      assert sql_verify.success, test_case
      assert len(sql_verify.data) == 1, "did not find query '{0}' in query log " \
                                        "table for test case '{1}" \
                                        .format(sql_result.query_id, test_case)
      assert sql_verify.data[0] == sql, test_case
    else:
      esc_sql = sql.replace("'", "\\'")
      sql_verify = client.execute("select sql from {0} where sql='{1}' "
                                  "and start_time_utc > "
                                  "date_sub(utc_timestamp(), interval 25 seconds);"
                                  .format(self.QUERY_TBL, esc_sql))
      assert sql_verify.success, test_case
      assert len(sql_verify.data) == 1, "did not find query '{0}' in query log " \
                                        "table for test case '{1}" \
                                        .format(esc_sql, test_case)

  LOG_DIR_MAX_WRITES = tempfile.mkdtemp(prefix="max_writes")

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=5 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60 "
                                                 "--log_dir={0}"
                                                 .format(LOG_DIR_MAX_WRITES),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_max_attempts_exceeded(self, vector):
    """Asserts that completed queries are only attempted 3 times to be inserted into the
       completed queries table. This test deletes the completed queries table thus it must
       not come last otherwise the table stays deleted. Subsequent tests will re-create
       the table."""

    print("USING LOG DIRECTORY: {0}".format(self.LOG_DIR_MAX_WRITES))

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      res = client.execute("drop table {0} purge".format(self.QUERY_TBL))
      assert res.success
      impalad.service.wait_for_metric_value(
          "impala-server.completed_queries.scheduled_writes", 3, 60)
      impalad.service.wait_for_metric_value("impala-server.completed_queries.failure", 3,
          60)

      query_count = 0

      # Allow time for logs to be written to disk.
      sleep(5)

      with open(os.path.join(self.LOG_DIR_MAX_WRITES, "impalad.ERROR")) as file:
        for line in file:
          if line.find('could not write completed query table="{0}" query_id="{1}"'
                           .format(self.QUERY_TBL, res.query_id)) >= 0:
            query_count += 1

      assert query_count == 1

      assert impalad.service.get_metric_value(
        "impala-server.completed_queries.max_records_writes") == 0
      assert impalad.service.get_metric_value(
        "impala-server.completed_queries.queued") == 0
      assert impalad.service.get_metric_value(
        "impala-server.completed_queries.failure") == 3
      assert impalad.service.get_metric_value(
        "impala-server.completed_queries.scheduled_writes") == 4
      assert impalad.service.get_metric_value(
        "impala-server.completed_queries.written") == 0
    finally:
      client.close()

  FLUSH_INTERNAL_CLUSTER_ID = "test_query_hist_interval_" + str(int(time()))

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=60 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60 "
                                                 "--cluster_id={0}"
                                                 .format(FLUSH_INTERNAL_CLUSTER_ID),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_table_flush_interval(self, vector):
    """Asserts that queries that have completed are written to the query log table
       after the specified write interval elapses."""

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      query_count = 0
      start_time = time()

      while (time() - start_time) < 58:
        res = client.execute("select sleep(750)")
        assert res.success
        query_count += 1

      def assert_func(last_iteration):
        results = client.execute("select count(*) from {0} where cluster_id='{1}'"
                                 .format(self.QUERY_TBL, self.FLUSH_INTERNAL_CLUSTER_ID))

        success = len(results.data) == 1
        if last_iteration:
          assert len(results.data) == 1

        if success:
          data = results.data[0].split("\t")
          assert data[0] >= query_count

        return success

      assert retry(assert_func)
    finally:
      client.close()

  FLUSH_INTERNAL_CLUSTER_ID = "test_query_log_max_records_" + str(int(time()))
  QUERY_COUNT = 2

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_max_queued={0} "
                                                 "--query_log_write_interval_s=9999 "
                                                 "--shutdown_grace_period_s=10 "
                                                 "--shutdown_deadline_s=60 "
                                                 "--cluster_id={1}".format(QUERY_COUNT,
                                                 FLUSH_INTERNAL_CLUSTER_ID),
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=True)
  def test_query_log_flush_max_records(self, vector):
    """Asserts that queries that have completed are written to the query log table when
       the maximum number of queued records it reached."""

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    test_sql = "select '{0}'".format(self.FLUSH_INTERNAL_CLUSTER_ID)
    test_sql_assert = "select count(*) from {0} where sql='select \\'{1}\\''" \
                      .format(self.QUERY_TBL, self.FLUSH_INTERNAL_CLUSTER_ID)

    try:
      for _ in range(0, self.QUERY_COUNT):
        res = client.execute(test_sql)
        assert res.success

      # Running this query results in the number of queued completed queries to exceed
      # the max and thus all completed queries will be written to the query log table.
      res = client.execute(test_sql_assert)
      assert res.success
      assert 1 == len(res.data)
      assert "0" == res.data[0]

      # Wait until the completed queries have all been written out because the max queued
      # count was exceeded.
      impalad.service.wait_for_metric_value(
          "impala-server.completed_queries.max_records_writes", 1, 60)

      # Allow time for Impala to process the insert.
      # TODO - can this sleep be eliminated in favor of retries that
      #        assert minimum counts instead of exact counts?
      sleep(30)

      # This query will remain queued due to the long write interval and max queued
      # records limit not being reached.
      res = client.execute("select count(*) from {0} where cluster_id='{1}'"
                          .format(self.QUERY_TBL, self.FLUSH_INTERNAL_CLUSTER_ID))
      assert res.success
      assert 1 == len(res.data)
      assert "3" == res.data[0]
      impalad.service.wait_for_metric_value(
          "impala-server.completed_queries.queued", 1, 60)
    finally:
      client.close()

    assert impalad.service.get_metric_value(
        "impala-server.completed_queries.max_records_writes") == 1
    assert impalad.service.get_metric_value(
        "impala-server.completed_queries.scheduled_writes") == 0
    assert impalad.service.get_metric_value(
        "impala-server.completed_queries.written") == self.QUERY_COUNT + 1
    assert impalad.service.get_metric_value(
        "impala-server.completed_queries.queued") == 1

  @CustomClusterTestSuite.with_args(impalad_args="--enable_workload_mgmt "
                                                 "--query_log_write_interval_s=9999 "
                                                 "--shutdown_grace_period_s=30 "
                                                 "--shutdown_deadline_s=30",
                                    catalogd_args="--enable_workload_mgmt",
                                    impalad_graceful_shutdown=False)
  def test_query_log_table_flush_on_shutdown(self, vector):
    """Asserts that queries that have completed but are not yet written to the query
       log table are flushed to the table before the coordinator exits."""

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()

    try:
      # Execute sql statements to ensure all get written to the query log table.
      sql1 = client.execute("select 1")
      assert sql1.success

      sql2 = client.execute("select 2")
      assert sql2.success

      sql3 = client.execute("select 3")
      assert sql3.success

      impalad.service.wait_for_metric_value("impala-server.completed_queries.queued", 3,
          60)

      impalad.kill_and_wait_for_exit(SIGRTMIN)

      client2 = self.create_client_for_nth_impalad(1)

      def assert_func(last_iteration):
        results = client2.execute("select query_id,sql from {0} where query_id in "
                                  "('{1}','{2}','{3}')".format(self.QUERY_TBL,
                                  sql1.query_id, sql2.query_id, sql3.query_id))

        success = len(results.data) == 3
        if last_iteration:
          assert len(results.data) == 3

        return success

      assert retry(func=assert_func, max_attempts=5, sleep_time_s=5)
    finally:
      client.close()
      client2.close()


def assert_scan_node_metrics(re_metric, profile_lines):
  """Retrieves metrics reported under HDFS_SCAN_NODEs removing any metrics from
      Averaged Fragments. The provided re_metric must be a compiled regular expression
      with at least one capture group. Returns a list of the contents of the first
      capture group in the re_metrics regular expression for all matching metrics."""
  metrics = []

  re_in_scan = re.compile(r'^\s+HDFS_SCAN_NODE')
  re_avg_fgmt = re.compile(r'^(\s+)Averaged Fragment')
  in_scan = False
  in_avg_fgmt = 0
  for line in profile_lines:
    avg_fmt_res = re_avg_fgmt.search(line)
    if avg_fmt_res is not None:
      # Averaged Fragments sometimes have HDFS_SCAN_NODEs which must be skipped.
      in_avg_fgmt = len(avg_fmt_res.group(1))
    elif in_avg_fgmt > 0 and line[in_avg_fgmt + 1] != " ":
      # Found a line at the same indentation as the previous Averaged Fragement, thus
      # we successfully skipped over any HDFS_SCAN_NODEs if they existed.
      in_avg_fgmt = 0
    elif in_avg_fgmt == 0 and re_in_scan.match(line) is not None:
      # Found a HDFS_SCAN_NODE that was not under an Averaged Fragment.
      in_scan = True
    elif in_scan:
      # Search through the HDFS_SCAN_NODE for the metric.
      res = re_metric.search(line)
      if res is not None:
        metrics.append(res.group(1))
        in_scan = False

  return metrics
