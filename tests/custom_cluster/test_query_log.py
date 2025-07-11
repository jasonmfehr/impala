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
from getpass import getuser
import os
from random import choice, randint
from signal import SIGRTMIN
import string
from time import sleep, time

from thrift.protocol import TBinaryProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport

from impala_thrift_gen.ImpalaService import ImpalaHiveServer2Service
from impala_thrift_gen.TCLIService import TCLIService
from tests.common.cluster_config import impalad_admission_ctrl_config_args
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_connection import FINISHED
from tests.common.impala_test_suite import IMPALAD_HS2_HOST_PORT
from tests.common.skip import SkipIfExploration
from tests.common.test_dimensions import hs2_client_protocol_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.common.wm_test_suite import WorkloadManagementTestSuite
from tests.util.retry import retry
from tests.util.workload_management import (
    assert_query,
    QUERY_TBL_LOG,
    redaction_rules_file,
    WM_DB,
)


class TestQueryLogTableBasic(WorkloadManagementTestSuite):
  """Tests to assert the query log table is correctly populated when using the Beeswax
     client protocol."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableBasic, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(hs2_client_protocol_dimension())

  MAX_SQL_PLAN_LEN = 2000
  LOG_DIR_MAX_WRITES = 'max_attempts_exceeded'

  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_max_select "
                                                 "--query_log_max_sql_length={0} "
                                                 "--query_log_max_plan_length={0}"
                                                 .format(MAX_SQL_PLAN_LEN),
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_lower_max_sql_plan(self, vector):
    """Asserts that length limits on the sql and plan columns in the completed queries
       table are respected."""
    client = self.get_client(vector.get_value('protocol'))
    rand_long_str = "".join(choice(string.ascii_letters) for _ in
        range(self.MAX_SQL_PLAN_LEN))

    # Run the query async to avoid fetching results since fetching such a large result was
    # causing the execution to take a very long time.
    handle = client.execute_async("select '{0}'".format(rand_long_str))
    query_id = client.handle_id(handle)
    client.wait_for_finished_timeout(handle, 10)
    client.close_query(handle)

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + QUERY_TBL_LOG)

    res = client.execute("select length(sql),plan from {0} where query_id='{1}'"
        .format(QUERY_TBL_LOG, query_id))
    assert res.success
    assert len(res.data) == 1

    data = res.data[0].split("\t")
    assert len(data) == 2
    assert int(data[0]) == self.MAX_SQL_PLAN_LEN - 1, "incorrect sql statement length"
    assert len(data[1]) == self.MAX_SQL_PLAN_LEN - data[1].count("\n") - 1, \
        "incorrect plan length"

  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_max_select",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_sql_plan_too_long(self, vector):
    """Asserts that very long queries have their corresponding plan and sql columns
       shortened in the completed queries table."""
    client = self.get_client(vector.get_value('protocol'))
    rand_long_str = "".join(choice(string.ascii_letters) for _ in range(16778200))

    client.set_configuration_option("MAX_STATEMENT_LENGTH_BYTES", 16780000)
    handle = client.execute_async("select '{0}'".format(rand_long_str))
    query_id = client.handle_id(handle)
    client.wait_for_finished_timeout(handle, 10)
    client.close_query(handle)

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + QUERY_TBL_LOG)

    client.set_configuration_option("MAX_ROW_SIZE", 35000000)
    res = client.execute("select length(sql),plan from {0} where query_id='{1}'"
        .format(QUERY_TBL_LOG, query_id))
    assert res.success
    assert len(res.data) == 1
    data = res.data[0].split("\t")
    assert len(data) == 2
    assert data[0] == "16777215"

    # Newline characters are not counted by Impala's length function.
    assert len(data[1]) == 16777216 - data[1].count("\n") - 1

  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_1 "
                                                 "--query_log_size=0 "
                                                 "--query_log_size_in_bytes=0",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_no_query_log(self, vector):
    """Asserts queries are written to the completed queries table when the in-memory
       query log queue is turned off."""
    client = self.get_client(vector.get_value('protocol'))

    # Run a select query.
    random_val = randint(1, 1000000)
    select_sql = "select {0}".format(random_val)
    res = client.execute(select_sql)
    assert res.success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + QUERY_TBL_LOG)

    actual = client.execute("select sql from {0} where query_id='{1}'".format(
        QUERY_TBL_LOG, res.query_id))
    assert actual.success
    assert len(actual.data) == 1
    assert actual.data[0] == select_sql

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_2 "
                                                 "--always_use_data_cache "
                                                 "--data_cache={query_data_cache}:5GB",
                                    workload_mgmt=True,
                                    cluster_size=1,
                                    tmp_dir_placeholders=['query_data_cache'],
                                    disable_log_buffering=True)
  def test_query_data_cache(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile. Specifically focuses on the data cache metrics."""
    client = self.get_client(vector.get_value('protocol'))

    # Select all rows from the test table. Run the query multiple times to ensure data
    # is cached.
    warming_query_count = 3
    select_sql = "select * from functional.tinytable"
    for i in range(warming_query_count):
      res = client.execute(select_sql)
      assert res.success
      self.cluster.get_first_impalad().service.wait_for_metric_value(
          "impala-server.completed-queries.written", i + 1, 60)

    # Wait for the cache to be written to disk.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.io-mgr.remote-data-cache-num-writes", 1, 60)

    # Run the same query again so results are read from the data cache.
    res = client.execute(select_sql, fetch_profile_after_close=True)
    assert res.success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", warming_query_count + 1, 60)

    data = assert_query(QUERY_TBL_LOG, client, "test_query_hist_2",
        res.runtime_profile)

    # Since the assert_query function only asserts that the bytes read from cache
    # column is equal to the bytes read from cache in the profile, there is a potential
    # for this test to not actually assert anything different than other tests. Thus, an
    # additional assert is needed to ensure that there actually was data read from the
    # cache.
    assert data["BYTES_READ_CACHE_TOTAL"] != "0", "bytes read from cache total was " \
        "zero, test did not assert anything"

  @CustomClusterTestSuite.with_args(impalad_args="--query_log_write_interval_s=5",
                                    impala_log_dir=("{" + LOG_DIR_MAX_WRITES + "}"),
                                    workload_mgmt=True,
                                    tmp_dir_placeholders=[LOG_DIR_MAX_WRITES],
                                    disable_log_buffering=True)
  def test_max_attempts_exceeded(self, vector):
    """Asserts that completed queries are only attempted 3 times to be inserted into the
       completed queries table. This test deletes the completed queries table thus it must
       not come last otherwise the table stays deleted. Subsequent tests will re-create
       the table."""

    log_dir = self.get_tmp_dir(self.LOG_DIR_MAX_WRITES)
    print("USING LOG DIRECTORY: {0}".format(log_dir))

    impalad = self.cluster.get_first_impalad()
    client = self.get_client(vector.get_value('protocol'))

    res = client.execute("drop table {0} purge".format(QUERY_TBL_LOG))
    assert res.success
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.scheduled-writes", 3, 60)
    impalad.service.wait_for_metric_value("impala-server.completed-queries.failure", 3,
        60)

    query_count = 0

    # Allow time for logs to be written to disk.
    sleep(5)

    with open(os.path.join(log_dir, "impalad.ERROR")) as file:
      for line in file:
        if line.find('could not write completed query table="{0}" query_id="{1}"'
                          .format(QUERY_TBL_LOG, res.query_id)) >= 0:
          query_count += 1

    assert query_count == 1

    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.max-records-writes") == 0
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.queued") == 0
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.failure") == 3
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.scheduled-writes") == 4
    assert impalad.service.get_metric_value(
      "impala-server.completed-queries.written") == 0

  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(cluster_size=3,
                                    num_exclusive_coordinators=2,
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_dedicated_coordinator_no_mt_dop(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile when dedicated coordinators are used."""
    client = self.get_client(vector.get_value('protocol'))
    test_sql = "select * from functional.tinytable"

    # Select all rows from the test table.
    res = client.execute(test_sql, fetch_profile_after_close=True)
    assert res.success

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(1, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(QUERY_TBL_LOG, client2, "",
          res.runtime_profile)
    finally:
      client2.close()

  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(cluster_size=3,
                                    num_exclusive_coordinators=2,
                                    workload_mgmt=True,
                                    disable_log_buffering=True,
                                    force_restart=True)
  def test_dedicated_coordinator_with_mt_dop(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile when dedicated coordinators are used along with an MT_DOP setting
       greater than 0."""
    client = self.get_client(vector.get_value('protocol'))
    test_sql = "select * from functional.tinytable"

    # Select all rows from the test table.
    client.set_configuration_option("MT_DOP", "4")
    res = client.execute(test_sql, fetch_profile_after_close=True)
    assert res.success

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(1, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(QUERY_TBL_LOG, client2, "",
          res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--redaction_rules_file={}"
                                                 .format(redaction_rules_file()),
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_redaction(self, vector):
    """Asserts the query log table redacts the statement."""
    client = self.get_client(vector.get_value('protocol'))
    result = client.execute(
        "select *, 'supercalifragilisticexpialidocious' from functional.alltypes",
        fetch_profile_after_close=True)
    assert result.success

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)
    assert_query(QUERY_TBL_LOG, client, raw_profile=result.runtime_profile)


@SkipIfExploration.is_not_exhaustive()
class TestQueryLogOtherTable(WorkloadManagementTestSuite):
  """Tests to assert that query_log_table_name works with non-default value."""

  OTHER_TBL = "completed_queries_table_{0}".format(int(time()))

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogOtherTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(hs2_client_protocol_dimension())

  @CustomClusterTestSuite.with_args(impalad_args="--blacklisted_dbs=information_schema "
                                                 "--query_log_table_name={0}"
                                                 .format(OTHER_TBL),
                                    catalogd_args="--blacklisted_dbs=information_schema "
                                                  "--query_log_table_name={0}"
                                                  .format(OTHER_TBL),
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_renamed_log_table(self, vector):
    """Asserts that the completed queries table can be renamed."""

    client = self.get_client(vector.get_value('protocol'))

    try:
      res = client.execute("show tables in {0}".format(WM_DB))
      assert res.success
      assert len(res.data) > 0, "could not find any tables in database {0}" \
          .format(WM_DB)

      tbl_found = False
      for tbl in res.data:
        if tbl.startswith(self.OTHER_TBL):
          tbl_found = True
          break
      assert tbl_found, "could not find table '{0}' in database '{1}'" \
          .format(self.OTHER_TBL, WM_DB)
    finally:
      client.execute("drop table {0}.{1} purge".format(WM_DB, self.OTHER_TBL))


class TestQueryLogTableHS2(WorkloadManagementTestSuite):
  """Tests to assert the query log table is correctly populated when using the HS2
     client protocol."""

  HS2_OPERATIONS_CLUSTER_ID = "hs2-operations-" + str(int(time()))

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableHS2, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(hs2_client_protocol_dimension())

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id={}"
                                                 .format(HS2_OPERATIONS_CLUSTER_ID),
                                    cluster_size=2,
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_hs2_metadata_operations(self, vector):
    """Certain HS2 operations appear to Impala as a special kind of query. Specifically,
       these operations have a type of unknown and a normally invalid sql syntax. This
       test asserts those queries are not written to the completed queries table since
       they are trivial."""
    client = self.get_client(vector.get_value('protocol'))

    host, port = IMPALAD_HS2_HOST_PORT.split(":")
    socket = TSocket(host, port)
    transport = TBufferedTransport(socket)
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    hs2_client = ImpalaHiveServer2Service.Client(protocol)

    # Asserts the response from an HS2 operation indicates success.
    def assert_resp(resp):
      assert resp.status.statusCode == TCLIService.TStatusCode.SUCCESS_STATUS

    # Closes an HS2 operation.
    def close_op(client, resp):
      close_operation_req = TCLIService.TCloseOperationReq()
      close_operation_req.operationHandle = resp.operationHandle
      assert_resp(client.CloseOperation(close_operation_req))

    try:
      # Open a new HS2 session.
      open_session_req = TCLIService.TOpenSessionReq()
      open_session_req.username = getuser()
      open_session_req.configuration = dict()
      open_sess_resp = hs2_client.OpenSession(open_session_req)
      assert_resp(open_sess_resp)

      # Test the get_type_info query.
      get_typeinfo_req = TCLIService.TGetTypeInfoReq()
      get_typeinfo_req.sessionHandle = open_sess_resp.sessionHandle
      get_typeinfo_resp = hs2_client.GetTypeInfo(get_typeinfo_req)
      assert_resp(get_typeinfo_resp)
      close_op(hs2_client, get_typeinfo_resp)

      # Test the get_catalogs query.
      get_cats_req = TCLIService.TGetCatalogsReq()
      get_cats_req.sessionHandle = open_sess_resp.sessionHandle
      get_cats_resp = hs2_client.GetCatalogs(get_cats_req)
      assert_resp(get_cats_resp)
      close_op(hs2_client, get_cats_resp)

      # Test the get_schemas query.
      get_schemas_req = TCLIService.TGetSchemasReq()
      get_schemas_req.sessionHandle = open_sess_resp.sessionHandle
      get_schemas_resp = hs2_client.GetSchemas(get_schemas_req)
      assert_resp(get_schemas_resp)
      close_op(hs2_client, get_schemas_resp)

      # Test the get_tables query.
      get_tables_req = TCLIService.TGetTablesReq()
      get_tables_req.sessionHandle = open_sess_resp.sessionHandle
      get_tables_req.schemaName = WM_DB
      get_tables_resp = hs2_client.GetTables(get_tables_req)
      assert_resp(get_tables_resp)
      close_op(hs2_client, get_tables_resp)

      # Test the get_table_types query.
      get_tbl_typ_req = TCLIService.TGetTableTypesReq()
      get_tbl_typ_req.sessionHandle = open_sess_resp.sessionHandle
      get_tbl_typ_req.schemaName = WM_DB
      get_tbl_typ_resp = hs2_client.GetTableTypes(get_tbl_typ_req)
      assert_resp(get_tbl_typ_resp)
      close_op(hs2_client, get_tbl_typ_resp)

      # Test the get_columns query.
      get_cols_req = TCLIService.TGetColumnsReq()
      get_cols_req.sessionHandle = open_sess_resp.sessionHandle
      get_cols_req.schemaName = 'functional'
      get_cols_req.tableName = 'parent_table'
      get_cols_resp = hs2_client.GetColumns(get_cols_req)
      assert_resp(get_cols_resp)
      close_op(hs2_client, get_cols_resp)

      # Test the get_primary_keys query.
      get_pk_req = TCLIService.TGetPrimaryKeysReq()
      get_pk_req.sessionHandle = open_sess_resp.sessionHandle
      get_pk_req.schemaName = 'functional'
      get_pk_req.tableName = 'parent_table'
      get_pk_resp = hs2_client.GetPrimaryKeys(get_pk_req)
      assert_resp(get_pk_resp)
      close_op(hs2_client, get_pk_resp)

      # Test the get_cross_reference query.
      get_cr_req = TCLIService.TGetCrossReferenceReq()
      get_cr_req.sessionHandle = open_sess_resp.sessionHandle
      get_cr_req.parentSchemaName = "functional"
      get_cr_req.foreignSchemaName = "functional"
      get_cr_req.parentTableName = "parent_table"
      get_cr_req.foreignTableName = "child_table"
      get_cr_resp = hs2_client.GetCrossReference(get_cr_req)
      assert_resp(get_cr_resp)
      close_op(hs2_client, get_cr_resp)

      close_session_req = TCLIService.TCloseSessionReq()
      close_session_req.sessionHandle = open_sess_resp.sessionHandle
      resp = hs2_client.CloseSession(close_session_req)
      assert resp.status.statusCode == TCLIService.TStatusCode.SUCCESS_STATUS
    finally:
      socket.close()

    # Execute a general query and wait for it to appear in the completed queries table to
    # ensure there are no false positives caused by the assertion query executing before
    # Impala has a chance to write queued queries to the completed queries table.
    assert client.execute("select 1").success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
          "impala-server.completed-queries.written", 1, 30)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh {}".format(QUERY_TBL_LOG))

    # Assert only the one expected query was written to the completed queries table.
    assert_results = client.execute("select count(*) from {} where cluster_id='{}'"
        .format(QUERY_TBL_LOG, self.HS2_OPERATIONS_CLUSTER_ID))
    assert assert_results.success
    assert assert_results.data[0] == "1"

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_mult",
                                    cluster_size=2,
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_query_multiple_tables(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile for a query that reads from multiple tables."""
    client = self.get_client(vector.get_value('protocol'))

    # Select all rows from the test table.
    client.set_configuration_option("MAX_MEM_ESTIMATE_FOR_ADMISSION", "10MB")
    res = client.execute("select a.zip,a.income,b.timezone,c.timezone from "
        "functional.zipcode_incomes a inner join functional.zipcode_timezones b on "
        "a.zip = b.zip inner join functional.alltimezones c on b.timezone = c.timezone",
        fetch_profile_after_close=True)
    assert res.success
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(1, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(QUERY_TBL_LOG, client2, "test_query_hist_mult", res.runtime_profile,
          max_mem_for_admission=10485760)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_3",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_insert_select(self, vector, unique_database,
      unique_name):
    """Asserts the values written to the query log table match the values from the
       query profile for a query that insert selects."""
    tbl_name = "{0}.{1}".format(unique_database, unique_name)
    client = self.get_client(vector.get_value('protocol'))

    # Create the destination test table.
    assert client.execute("create table {0} (identifier INT, product_name STRING) "
        .format(tbl_name)).success, "could not create source table"

    # Insert select into the destination table.
    res = client.execute("insert into {0} (identifier, product_name) select id, "
        "string_col from functional.alltypes limit 50".format(tbl_name),
        fetch_profile_after_close=True)
    assert res.success, "could not insert select"

    # Include the two queries run by the unique_database fixture setup.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 4, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(QUERY_TBL_LOG, client2, "test_query_hist_3", res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(
      impalad_args="--query_log_write_interval_s=3 "
      "--query_log_dml_exec_timeout_s=1 "
      "--debug_actions=INTERNAL_SERVER_AFTER_SUBMIT:SLEEP@2000",
      workload_mgmt=True,
      cluster_size=1, disable_log_buffering=True)
  def test_exec_timeout(self, vector):
    """Asserts the --query_log_dml_exec_timeout_s startup flag is added to the workload
       management insert DML and the DML will be cancelled when its execution time exceeds
       the value of the startup flag. Also asserts the workload management code
       detects the query was cancelled and handles the DML failure properly."""
    client = self.get_client(vector.get_value('protocol'))

    # Run a query to completion so the completed queries queue has 1 entry.
    assert client.execute("select 1").success

    # Helper function that waits for the workload management insert DML to start.
    def wait_for_insert_query():
      self.insert_query_id = _find_query_in_ui(self.cluster.get_first_impalad().service,
          "in_flight_queries", _is_insert_query)
      return self.insert_query_id

    assert \
        retry(func=wait_for_insert_query, max_attempts=10, sleep_time_s=1, backoff=1), \
        "did not find completed queries insert dml in the debug web ui"
    self.assert_impalad_log_contains("INFO", "Expiring query {} due to execution time "
                                     "limit of 1s.".format(self.insert_query_id))

    # When a workload management insert DML fails, a warning message is logged that
    # contains information about the DML. These two assertions ensure the message is
    # logged and important fields are correct.
    res = self.assert_impalad_log_contains(
        level="WARNING",
        line_regex=r'failed to write completed queries table="{}" record_count="(\d+)" '
                   r'bytes="\S+\s\S+" gather_time="\S+" exec_time="\S+" query_id="{}" '
                   r'msg="(.*?)"'.format(QUERY_TBL_LOG, self.insert_query_id),
        expected_count=-1)
    assert res.group(1) == "1", "Invalid record count in the query failed log line"
    assert res.group(2) == "Query {0} expired due to execution time limit of 1s000ms" \
                           .format(self.insert_query_id), \
                           "Found expected query failed log line but the msg parameter " \
                           "was incorrect"

    self.assert_impalad_log_contains(level="INFO",
                                     line_regex="Query {} expired due to execution time "
                                     "limit of 1s000ms".format(self.insert_query_id),
                                     expected_count=2)


class TestQueryLogTableAll(WorkloadManagementTestSuite):
  """Tests to assert the query log table is correctly populated when using all the
     client protocols."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableAll, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(hs2_client_protocol_dimension())

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_2",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_ddl(self, vector, unique_database, unique_name):
    """Asserts the values written to the query log table match the values from the
       query profile for a DDL query."""
    create_tbl_sql = "create table {0}.{1} (id INT, product_name STRING) " \
        "partitioned by (category INT)".format(unique_database, unique_name)
    client = self.get_client(vector.get_value('protocol'))

    res = client.execute(create_tbl_sql, fetch_profile_after_close=True)
    assert res.success

    # Include the two queries run by the unique_database fixture setup.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 3, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(QUERY_TBL_LOG, client2, "test_query_hist_2", res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_3",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_dml(self, vector, unique_database, unique_name):
    """Asserts the values written to the query log table match the values from the
       query profile for a DML query."""
    tbl_name = "{0}.{1}".format(unique_database, unique_name)
    client = self.get_client(vector.get_value('protocol'))

    # Create the test table.
    create_tbl_sql = "create table {0} (id INT, product_name STRING) " \
      "partitioned by (category INT)".format(tbl_name)
    create_tbl_results = client.execute(create_tbl_sql)
    assert create_tbl_results.success

    insert_sql = "insert into {0} (id,category,product_name) values " \
                  "(0,1,'the product')".format(tbl_name)
    res = client.execute(insert_sql, fetch_profile_after_close=True)
    assert res.success

    # Include the two queries run by the unique_database fixture setup.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 4, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      assert_query(QUERY_TBL_LOG, client2, "test_query_hist_3", res.runtime_profile)
    finally:
      client2.close()

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_2",
                                    workload_mgmt=True,
                                    disable_log_buffering=True,
                                    force_restart=True)
  def test_invalid_query(self, vector):
    """Asserts correct values are written to the completed queries table for a failed
       query. The query profile is used as the source of expected values."""
    client = self.get_client(vector.get_value('protocol'))

    # Assert an invalid query
    unix_now = time()
    try:
      client.execute("{0}".format(unix_now))
    except Exception:
      pass

    # Get the query id from the completed queries table since the call to execute errors
    # instead of return the results object which contains the query id.
    impalad = self.cluster.get_first_impalad()
    impalad.service.wait_for_metric_value("impala-server.completed-queries.written", 1,
        60)

    result = client.execute("select query_id from {0} where sql='{1}'"
                            .format(QUERY_TBL_LOG, unix_now),
                            fetch_profile_after_close=True)
    assert result.success
    assert len(result.data) == 1

    assert_query(query_tbl=QUERY_TBL_LOG, client=client,
        expected_cluster_id="test_query_hist_2", impalad=impalad, query_id=result.data[0])

  @CustomClusterTestSuite.with_args(workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_ignored_sqls_not_written(self, vector):
    """Asserts that expected queries are not written to the query log table."""
    client = self.get_client(vector.get_value('protocol'))

    sqls = {}
    sqls["use default"] = False
    sqls["USE default"] = False
    sqls["uSe default"] = False
    sqls["--mycomment\nuse default"] = False
    sqls["/*mycomment*/ use default"] = False

    sqls["set all"] = False
    sqls["SET all"] = False
    sqls["SeT all"] = False
    sqls["--mycomment\nset all"] = False
    sqls["/*mycomment*/ set all"] = False

    sqls["show tables"] = False
    sqls["SHOW tables"] = False
    sqls["ShoW tables"] = False
    sqls["ShoW create table {0}".format(QUERY_TBL_LOG)] = False
    sqls["show databases"] = False
    sqls["SHOW databases"] = False
    sqls["ShoW databases"] = False
    sqls["show schemas"] = False
    sqls["SHOW schemas"] = False
    sqls["ShoW schemas"] = False
    sqls["--mycomment\nshow tables"] = False
    sqls["/*mycomment*/ show tables"] = False
    sqls["/*mycomment*/ show tables"] = False
    sqls["/*mycomment*/ show create table {0}".format(QUERY_TBL_LOG)] = False
    sqls["/*mycomment*/ show files in {0}".format(QUERY_TBL_LOG)] = False
    sqls["/*mycomment*/ show functions"] = False
    sqls["/*mycomment*/ show data sources"] = False
    sqls["/*mycomment*/ show views"] = False
    sqls["show metadata tables in {0}".format(QUERY_TBL_LOG)] = False

    sqls["describe database default"] = False
    sqls["/*mycomment*/ describe database default"] = False
    sqls["describe {0}".format(QUERY_TBL_LOG)] = False
    sqls["/*mycomment*/ describe {0}".format(QUERY_TBL_LOG)] = False
    sqls["describe history {0}".format(QUERY_TBL_LOG)] = False
    sqls["/*mycomment*/ describe history {0}".format(QUERY_TBL_LOG)] = False
    sqls["select 1"] = True

    control_queries_count = 0
    # Note: This needs to iterate over a copy of sqls.items(), because it modifies
    # sqls as it iterates.
    for sql, experiment_control in list(sqls.items()):
      results = client.execute(sql)
      assert results.success, "could not execute query '{0}'".format(sql)
      sqls[sql] = results.query_id

      # Ensure at least one sql statement was written to the completed queries table
      # to avoid false negatives where the sql statements that are ignored are not
      # written to the completed queries table because of another issue. Does not check
      # the completed-queries.written metric because, if another query that should not
      # have been written to the completed queries was actually written, the metric will
      # be wrong.
      if experiment_control:
        control_queries_count += 1
        sql_results = None
        for _ in range(6):
          sql_results = client.execute("select * from {0} where query_id='{1}'".format(
            QUERY_TBL_LOG, results.query_id))
          control_queries_count += 1
          if sql_results.success and len(sql_results.data) == 1:
            break
          else:
            # The query is not yet available in the completed queries table, wait before
            # checking again.
            sleep(5)
        assert sql_results.success
        assert len(sql_results.data) == 1, "query not found in completed queries table"
        sqls.pop(sql)

    for sql, query_id in sqls.items():
      log_results = client.execute("select * from {0} where query_id='{1}'"
                                    .format(QUERY_TBL_LOG, query_id))
      assert log_results.success
      assert len(log_results.data) == 0, "found query in query log table: {0}".format(sql)

    # Assert there was one query per sql item written to the query log table. The queries
    # inserted into the completed queries table are the queries used to assert the ignored
    # queries were not written to the table.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", len(sqls) + control_queries_count, 60)
    assert self.cluster.get_first_impalad().service.get_metric_value(
        "impala-server.completed-queries.failure") == 0

  @CustomClusterTestSuite.with_args(workload_mgmt=True,
                                    disable_log_buffering=True,
                                    force_restart=True)
  def test_sql_injection_attempts(self, vector):
    client = self.get_client(vector.get_value('protocol'))
    impalad = self.cluster.get_first_impalad()

    # Try a sql injection attack with closing double quotes.
    sql1_str = "select * from functional.alltypes where string_col='product-2-3\"'"
    self.__run_sql_inject(impalad, client, sql1_str, "closing quotes", 1)

    # Try a sql injection attack with closing single quotes.
    sql1_str = "select * from functional.alltypes where string_col=\"product-2-3'\""
    self.__run_sql_inject(impalad, client, sql1_str, "closing quotes", 4)

    # Try a sql inject attack with terminating quote and semicolon.
    sql2_str = "select 1'); drop table {0}; select('".format(QUERY_TBL_LOG)
    self.__run_sql_inject(impalad, client, sql2_str, "terminating semicolon", 7)

    # Attempt to cause an error using multiline comments.
    sql3_str = "select 1' /* foo"
    self.__run_sql_inject(impalad, client, sql3_str, "multiline comments", 11, False)

    # Attempt to cause an error using single line comments.
    sql4_str = "select 1' -- foo"
    self.__run_sql_inject(impalad, client, sql4_str, "single line comments", 15, False)

  def __run_sql_inject(self, impalad, client, sql, test_case, expected_writes,
                       expect_success=True):
    # Capture coordinators "now" so we match only queries in this test case.
    start_time = None
    if not expect_success:
      utc_timestamp = self.execute_query('select utc_timestamp()')
      assert len(utc_timestamp.data) == 1
      start_time = utc_timestamp.data[0]

    sql_result = None
    try:
      sql_result = client.execute(sql)
    except Exception as e:
      if expect_success:
        raise e

    if expect_success:
      assert sql_result.success

    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.written", expected_writes, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + QUERY_TBL_LOG)

    if expect_success:
      sql_verify = client.execute(
          "select sql from {0} where query_id='{1}'"
          .format(QUERY_TBL_LOG, sql_result.query_id))

      assert sql_verify.success, test_case
      assert len(sql_verify.data) == 1, "did not find query '{0}' in query log " \
                                        "table for test case '{1}" \
                                        .format(sql_result.query_id, test_case)
      assert sql_verify.data[0] == sql, test_case
    else:
      assert start_time is not None
      esc_sql = sql.replace("'", "\\'")
      sql_verify = client.execute("select sql from {0} where sql='{1}' "
                                  "and start_time_utc > '{2}'"
                                  .format(QUERY_TBL_LOG, esc_sql, start_time))
      assert sql_verify.success, test_case
      assert len(sql_verify.data) == 1, "did not find query '{0}' in query log " \
                                        "table for test case '{1}" \
                                        .format(esc_sql, test_case)


class TestQueryLogTableBufferPool(WorkloadManagementTestSuite):
  """Base class for all query log tests that set the buffer pool query option."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogTableBufferPool, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(hs2_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('buffer_pool_limit',
        None, "14.97MB"))

  @CustomClusterTestSuite.with_args(impalad_args="--cluster_id=test_query_hist_1 "
                                                 "--scratch_dirs={scratch_dir}:5G",
                                    workload_mgmt=True,
                                    tmp_dir_placeholders=['scratch_dir'],
                                    disable_log_buffering=True)
  def test_select(self, vector):
    """Asserts the values written to the query log table match the values from the
       query profile. If the buffer_pool_limit parameter is not None, then this test
       requires that the query spills to disk to assert that the spill metrics are correct
       in the completed queries table."""
    buffer_pool_limit = vector.get_value('buffer_pool_limit')
    client = self.get_client(vector.get_value('protocol'))
    test_sql = "select * from functional.tinytable"

    # When buffer pool limit is not None, the test is forcing the query to spill. Thus,
    # a large number of records is needed to force the spilling.
    if buffer_pool_limit is not None:
      test_sql = "select a.*,b.*,c.* from " \
        "functional.zipcode_incomes a inner join functional.zipcode_timezones b on " \
        "a.zip = b.zip inner join functional.alltimezones c on b.timezone = c.timezone"

    # Set up query configuration
    client.set_configuration_option("MAX_MEM_ESTIMATE_FOR_ADMISSION", "10MB")
    if buffer_pool_limit is not None:
      client.set_configuration_option("BUFFER_POOL_LIMIT", buffer_pool_limit)

    # Select all rows from the test table.
    res = client.execute(test_sql, fetch_profile_after_close=True)
    assert res.success

    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written", 1, 60)

    client2 = self.create_client_for_nth_impalad(2, vector.get_value('protocol'))
    try:
      assert client2 is not None
      data = assert_query(QUERY_TBL_LOG, client2, "test_query_hist_1",
          res.runtime_profile, max_mem_for_admission=10485760)
    finally:
      client2.close()

    if buffer_pool_limit is not None:
      # Since the assert_query function only asserts that the compressed bytes spilled
      # column is equal to the compressed bytes spilled in the profile, there is a
      # potential for this test to not actually assert anything different than other
      # tests. Thus, an additional assert is needed to ensure that there actually was
      # data that was spilled.
      assert data["COMPRESSED_BYTES_SPILLED"] != "0", "compressed bytes spilled total " \
          "was zero, test did not assert anything"


class TestQueryLogQueuedQueries(WorkloadManagementTestSuite):
  """Simulates a cluster that is under load and has queries that are queueing in
     admission control."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryLogQueuedQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(hs2_client_protocol_dimension())

  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(
          fs_allocation_file="fair-scheduler-one-query.xml",
          llama_site_file="llama-site-one-query.xml",
          additional_args="--query_log_write_interval_s=5"),
      workload_mgmt=True,
      num_exclusive_coordinators=1, cluster_size=2,
      default_query_options=[('fetch_rows_timeout_ms', '1000')])
  def test_query_queued(self):
    """Tests the situation where a cluster is under heavy load and the workload management
       insert DML is queued in admission control and has to wait for an admission control
       slot to become available.

       The overall flow is:
         1. Run a query to completion so the 'insert into sys.impala_query_log' DML runs.
         2. Start a query that will block the only available admission control slot.
         3. Wait for the 'insert into sys.impala_query_log' DML to start.
         4. Wait 2 seconds to ensure the 1 second FETCH_ROWS_TIMEOUT_MS does not cause the
              insert query to fail.
         5. Fetch all rows of the original blocking query to cause it to finish and
              release its admission control slot.
         6. Assert the 'insert into sys.impala_query_log' DML is in the list of completed
              queries.
       """
    resp = self.hs2_client.execute_async("SELECT * FROM functional.alltypes LIMIT 5")
    self.hs2_client.wait_for_impala_state(resp, FINISHED, 60)
    self.hs2_client.close_query(resp)

    # Start a query that will consume the only available slot since its rows are not
    # fetched until later.
    ROW_LIMIT = 5
    long_query_resp = self.hs2_client.execute_async(
        "SELECT * FROM functional.alltypes LIMIT {}".format(ROW_LIMIT))

    # Helper function that waits for the workload management insert DML to start.
    def wait_for_insert_query():
      self.insert_query_id = _find_query_in_ui(self.cluster.get_first_impalad().service,
          "in_flight_queries", _is_insert_query)
      return self.insert_query_id

    assert \
        retry(func=wait_for_insert_query, max_attempts=10, sleep_time_s=1, backoff=1), \
        "did not find completed queries insert dml in the debug web ui"

    # Wait 2 seconds to ensure the insert into DML is not killed by the fetch rows
    # timeout (set to 1 second in this test's annotations).
    sleep(2)

    # Helper function that checks if a query matches the workload management insert DML
    # that had to wait for the one admission control to become available.
    def is_insert_query_queryid(query):
      return query["query_id"] == self.insert_query_id

    # Assert the workload management insert DML did not get cancelled early and is still
    # waiting.
    assert _find_query_in_ui(self.cluster.get_first_impalad().service,
        "in_flight_queries", is_insert_query_queryid), \
        "Did not find the workload management insert into query having id '{}' in the " \
        "list of in-flight queries".format(self.insert_query_id)

    # Retrieve all rows of the original blocking query to cause it to complete.
    self.hs2_client.wait_for_impala_state(long_query_resp, FINISHED, 60)
    self.hs2_client.close_query(long_query_resp)

    # Helper function that checks if a query matches the workload management insert DML
    # that had to wait for the one admission control slot to become available.
    def is_insert_query_queryid_success(query):
      return query["query_id"] == self.insert_query_id and query['state'] == "FINISHED"

    # Ensure the insert into DML has finished successfully. The previous row retrieval
    # ended the blocking query and thus should open up the one admission control slot for
    # the workload management DML to run.
    def find_completed_insert_query():
      return _find_query_in_ui(self.cluster.get_first_impalad().service,
          "completed_queries", is_insert_query_queryid_success)
    assert retry(func=find_completed_insert_query, max_attempts=10, sleep_time_s=1,
                 backoff=1)


class TestQueryLogTableFlush(CustomClusterTestSuite):
  """Tests to assert the query log table flush correctly under some shutdown
  scenario. They are separated from others because test may stop impalad individually.
  This test class does not extend from WorkloadManagementTestSuite because it must not
  wait until workload management is idle to begin graceful shutdown."""

  FLUSH_MAX_RECORDS_CLUSTER_ID = "test_query_log_flush_max_records_" + str(int(time()))
  FLUSH_MAX_RECORDS_QUERY_COUNT = 30

  def setup_method(self, method):
    super(TestQueryLogTableFlush, self).setup_method(method)
    self.wait_for_wm_init_complete()

  @CustomClusterTestSuite.with_args(impalad_args="--query_log_max_queued={0} "
                                                 "--query_log_write_interval_s=9999 "
                                                 "--cluster_id={1} "
                                                 "--query_log_expression_limit=5000"
                                                 .format(FLUSH_MAX_RECORDS_QUERY_COUNT,
                                                 FLUSH_MAX_RECORDS_CLUSTER_ID),
                                    default_query_options=[
                                      ('statement_expression_limit', 1024)],
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_flush_on_queued_count_exceeded(self):
    """Asserts that queries that have completed are written to the query log table when
       the maximum number of queued records is reached. Also verifies that writing
       completed queries is not limited by default statement_expression_limit."""

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_hs2_client()

    rand_str = "{0}-{1}".format(client.get_test_protocol(), time())

    test_sql = "select '{0}','{1}'".format(rand_str,
        self.FLUSH_MAX_RECORDS_CLUSTER_ID)
    test_sql_assert = "select '{0}', count(*) from {1} where sql='{2}'".format(
        rand_str, QUERY_TBL_LOG, test_sql.replace("'", r"\'"))

    for _ in range(0, self.FLUSH_MAX_RECORDS_QUERY_COUNT):
      res = client.execute(test_sql)
      assert res.success

    # Running this query results in the number of queued completed queries to exceed
    # the max and thus all completed queries will be written to the query log table.
    res = client.execute(test_sql_assert)
    assert res.success
    assert 1 == len(res.data)
    assert "0" == res.data[0].split("\t")[1]

    # Wait until the completed queries have all been written out because the max queued
    # count was exceeded.
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.max-records-writes", 1, 60)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.completed-queries.written",
        self.FLUSH_MAX_RECORDS_QUERY_COUNT + 1, 60)

    # Force Impala to process the inserts to the completed queries table.
    client.execute("refresh " + QUERY_TBL_LOG)

    # This query will remain queued due to the long write interval and max queued
    # records limit not being reached.
    res = client.execute(r"select count(*) from {0} where sql like 'select \'{1}\'%'"
        .format(QUERY_TBL_LOG, rand_str))
    assert res.success
    assert 1 == len(res.data)
    assert str(self.FLUSH_MAX_RECORDS_QUERY_COUNT + 1) == res.data[0]
    impalad.service.wait_for_metric_value(
        "impala-server.completed-queries.queued", 2, 60)

    assert impalad.service.get_metric_value(
        "impala-server.completed-queries.max-records-writes") == 1
    assert impalad.service.get_metric_value(
        "impala-server.completed-queries.scheduled-writes") == 0
    assert impalad.service.get_metric_value("impala-server.completed-queries.written") \
        == self.FLUSH_MAX_RECORDS_QUERY_COUNT + 1
    assert impalad.service.get_metric_value(
        "impala-server.completed-queries.queued") == 2

  @CustomClusterTestSuite.with_args(impalad_args="--query_log_write_interval_s=15",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_flush_on_interval(self):
    """Asserts that queries that have completed are written to the query log table
       after the specified write interval elapses."""

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_hs2_client()

    query_count = 10

    for i in range(query_count):
      res = client.execute("select sleep(1000)")
      assert res.success

    # Wait for at least one iteration of the workload management processing loop to write
    # to the completed queries table.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
      "impala-server.completed-queries.written", query_count, 20)

    # Helper function that waits for the workload management insert DML to start.
    def wait_for_insert_query():
      self.insert_query_id = _find_query_in_ui(self.cluster.get_first_impalad().service,
          "completed_queries", _is_insert_query)
      return self.insert_query_id

    # Wait for the workload management insert dml to be in the debug ui's completed
    # queries section so that it's query id can be retrieved.
    assert \
        retry(func=wait_for_insert_query, max_attempts=10, sleep_time_s=1, backoff=1), \
        "did not find completed queries insert dml in the debug web ui"
    self.assert_impalad_log_contains("INFO", r'wrote completed queries '
                                     r'table="{}" record_count="\d+" bytes="\S+\s?\S*" '
                                     r'gather_time="\S+" exec_time="\S+" query_id="{}"'
                                     .format(QUERY_TBL_LOG, self.insert_query_id))

  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(impalad_args="--query_log_write_interval_s=9999 "
                                                 "--shutdown_grace_period_s=0 "
                                                 "--shutdown_deadline_s=15 "
                                                 "--debug_actions="
                                                 "WM_SHUTDOWN_DELAY:SLEEP@5000",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_flush_on_shutdown(self):
    """Asserts that queries that have completed but are not yet written to the query
       log table are flushed to the table before the coordinator exits. Graceful shutdown
       for 2nd coordinator not needed because query_log_write_interval_s is very long."""

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_hs2_client()

    # Execute sql statements to ensure all get written to the query log table.
    sql1 = client.execute("select 1")
    assert sql1.success

    sql2 = client.execute("select 2")
    assert sql2.success

    sql3 = client.execute("select 3")
    assert sql3.success

    impalad.service.wait_for_metric_value("impala-server.completed-queries.queued", 3,
        60)

    impalad.kill_and_wait_for_exit(SIGRTMIN)
    self.assert_impalad_log_contains("INFO", r'Workload management shutdown successful',
      timeout_s=60)

    with self.cluster.impalads[1].service.create_hs2_client() as client2:
      def assert_func():
        results = client2.execute("select query_id,sql from {0} where query_id in "
                                  "('{1}','{2}','{3}')".format(QUERY_TBL_LOG,
                                  sql1.query_id, sql2.query_id, sql3.query_id))

        return len(results.data) == 3

      assert retry(func=assert_func, max_attempts=5, sleep_time_s=3)

  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(impalad_args="--query_log_write_interval_s=9999 "
                                                 "--shutdown_grace_period_s=0 "
                                                 "--query_log_shutdown_timeout_s=3 "
                                                 "--shutdown_deadline_s=15 "
                                                 "--debug_actions="
                                                 "WM_SHUTDOWN_DELAY:SLEEP@10000",
                                    workload_mgmt=True,
                                    disable_log_buffering=True)
  def test_shutdown_flush_timed_out(self):
    """Asserts that queries that have completed but are not yet written to the query
       log table are lost if the completed queries queue drain takes too long and that
       the coordinator logs the estimated number of queries lost."""

    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_hs2_client()

    # Execute sql statements to ensure all get written to the query log table.
    sql1 = client.execute("select 1")
    assert sql1.success

    sql2 = client.execute("select 2")
    assert sql2.success

    sql3 = client.execute("select 3")
    assert sql3.success

    impalad.service.wait_for_metric_value("impala-server.completed-queries.queued", 3,
        60)

    impalad.kill_and_wait_for_exit(SIGRTMIN)
    self.assert_impalad_log_contains("INFO", r"Workload management shutdown timed out. "
      r"Up to '3' queries may have been lost",
      timeout_s=60)


# Helper function to determine if a query from the debug UI is a workload management
# insert DML.
def _is_insert_query(query):
  return query["stmt"].lower().startswith("insert into {}".format(QUERY_TBL_LOG))


def _find_query_in_ui(service, section, func):
  """Calls to the debug UI's queries page and loops over all queries in the specified
      section calling the provided func for each query, Returns the string id of the
      first query that matches or None if no query matches."""
  assert section == "completed_queries" or section == "in_flight_queries"

  queries_json = service.get_debug_webpage_json('/queries')
  query_id = None

  for query in queries_json[section]:
    if func(query):
      query_id = query['query_id']
      break

  return query_id
