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

import json
import os
import random
import string
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import IMPALA_LOCAL_BUILD_VERSION
from tests.common.test_dimensions import hs2_client_protocol_dimension
from tests.util.workload_management import parse_db_user, parse_session_id

class TestOtelTrace(CustomClusterTestSuite):
  """Tests that exercise OpenTelemetry tracing behavior."""

  OUT_DIR = "out_dir"
  TRACE_FILE = "export-trace.jsonl"

  INIT_SPAN_IDX = 0
  SUBMITTED_SPAN_IDX = 1
  PLANNING_SPAN_IDX = 2
  ADMISSION_CONTROL_SPAN_IDX = 3
  QUERY_EXECUTION_SPAN_IDX = 4
  ROOT_SPAN_IDX = 5

  INIT_SPAN_ATTRS_COUNT = 16
  SUBMITTED_SPAN_ATTRS_COUNT = 8
  PLANNING_SPAN_ATTRS_COUNT = 9
  ROOT_SPAN_ATTRS_COUNT = 13

  @classmethod
  def add_test_dimensions(cls):
    super(TestOtelTrace, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(hs2_client_protocol_dimension())

  def setup_method(self, method):
    super(TestOtelTrace, self).setup_method(method)

  def __assert_json_path_value(self, obj, path, expected_value, message=None):
    """
    Assert that a path exists in a nested JSON object and has the expected value.
    
    Args:
        obj: The JSON object (dict/list) to check
        path: The path to check (e.g. "resourceSpans[0].scope.name")
        expected_value: The expected value at that path
        message: Optional custom message prefix for the assertion error
    """
    parts = path.replace('][', '].[').replace('[', '.[').split('.')
    current = obj
    
    # Navigate the path
    for part in parts:
        if not part:  # Skip empty parts
            continue
            
        if part.endswith(']'):  # Handle array indexing
            idx_start = part.find('[')
            idx_end = part.find(']', idx_start)
            if idx_start > 0:
                key = part[:idx_start]
                idx = int(part[idx_start+1:idx_end])
                assert key in current, "{}: '{}' not found in object".format(message or 'Path validation error', key)
                current = current[key]
                assert isinstance(current, list), "{}: '{}' is not a list".format(message or 'Path validation error', key)
                assert idx < len(current), "{}: Index {} out of range for '{}'".format(message or 'Path validation error', idx, key)
                current = current[idx]
            else:
                # Just an array index like [0]
                idx = int(part[1:idx_end])
                assert isinstance(current, list), "{}: Current object is not a list".format(message or 'Path validation error')
                assert idx < len(current), "{}: Index {} out of range".format(message or 'Path validation error', idx)
                current = current[idx]
        else:  # Handle regular dict keys
            assert isinstance(current, dict), "{}: Cannot access key '{}' in non-dict object".format(message or 'Path validation error', part)
            assert part in current, "{}: Key '{}' not found in object".format(message or 'Path validation error', part)
            current = current[part]
    
    # Check the value
    assert current == expected_value, "{}: Expected '{}', got '{}'".format(message or 'Path validation error', expected_value, current)
    return True
  
  @CustomClusterTestSuite.with_args(
      impalad_args="-v=2 --cluster_id=otel_trace --otel_trace_enabled=true "
                   "--otel_trace_exporter=file --otel_file_flush_interval_ms=500 "
                   "--otel_file_pattern={out_dir}/" + TRACE_FILE,
      cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
  def test_otel_trace(self):
    """Test that OpenTelemetry tracing is working by running a simple query and
    checking that the trace file is created and contains spans."""
    query = "select count(*) from tpch_parquet.lineitem"
    result = self.execute_query_expect_success(self.client, query)
    assert result.success
    query_id = result.query_id

    # Read the root span id from the Impalad logs.
    root_span_id, _ = self.find_span_log("root", query_id)

    # Wait until all spans are written to the trace file.
    obj = self.__wait_for_all_spans()

    # assert global resource attributes
    self.__assert_json_path_value(obj, "resourceSpans[0].resource.attributes[3].key", "service.version")
    self.__assert_json_path_value(obj, "resourceSpans[0].resource.attributes[3].value.stringValue", "5.0.0-SNAPSHOT")
    self.__assert_json_path_value(obj, "resourceSpans[0].resource.attributes[4].key", "service.name")
    self.__assert_json_path_value(obj, "resourceSpans[0].resource.attributes[4].value.stringValue", "Impala")

    # assert span level scope
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].scope.name", "org.apache.impala.impalad")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].scope.version", "5.0.0-SNAPSHOT")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[0].key", "Running")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[0].value.boolValue", False)

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[1].key", "UserName")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[1].value.stringValue", parse_db_user(result.runtime_profile))
    
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[2].key", "BeginTime")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[3].key", "SessionId")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[3].value.stringValue", parse_session_id(result.runtime_profile))

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[4].key", "QueryId")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[4].value.stringValue", query_id)

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[5].key", "Name")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[5].value.stringValue", "{} - Init".format(query_id))

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[6].key", "OriginalQueryId")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[6].value.stringValue", "")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[7].key", "DefaultDb")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[7].value.stringValue", "default")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[8].key", "RequestPool")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[8].value.stringValue", "default-pool")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[9].key", "QueryString")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[9].value.stringValue", query)

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[10].key", "ClusterId")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[10].value.stringValue", "otel_trace")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[11].key", "Status")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[11].value.stringValue", "TODO")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[12].key", "ErrorMsg")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[12].value.stringValue", "TODO")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[13].key", "StatusMessage")
    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[13].value.stringValue", "OK")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[14].key", "EndTime")

    self.__assert_json_path_value(obj, "resourceSpans[0].scopeSpans[0].spans[0].attributes[15].key", "ElapsedTime")


    # Assert the only top-level key is 'resourceSpans' and it is a 1 element list.
    cur_path = "resourceSpans"
    assert list(obj.keys()) == ["resourceSpans"], "Top level key expected: '{}', " \
        "actual: {}".format(cur_path, list(obj.keys()))
    assert isinstance(obj["resourceSpans"], list), "'{}' must be a list, actual: {}" \
        .format(cur_path, type(obj["resourceSpans"]))
    assert len(obj["resourceSpans"]) == 1, "'cur_path' must contain exactly one " \
        "element, actual: {}".format(cur_path, len(obj["resourceSpans"]))

    # Assert the structure of the resource field of the span.
    resource_attrs = obj["resourceSpans"][0]["resource"]["attributes"]
    cur_path = "resourceSpans[0].resource.attributes"
    assert isinstance(resource_attrs, list), "'{}' must be a list, actual: {}" \
        .format(cur_path, type(resource_attrs))
    assert len(resource_attrs) == 5, "'{}}' must contain exactly 5 elements, actual: {}" \
        .format(cur_path, len(resource_attrs))

    # Assert the resource attributes.
    assert_attr(resource_attrs, "service.name", "Impala", cur_path)
    assert_attr(resource_attrs, "service.version", IMPALA_LOCAL_BUILD_VERSION, cur_path)
    assert_attr(resource_attrs, "telemetry.sdk.version", \
        os.environ.get("IMPALA_OPENTELEMETRY_CPP_VERSION"), cur_path)
    assert_attr(resource_attrs, "telemetry.sdk.name", "opentelemetry", cur_path)
    assert_attr(resource_attrs, "telemetry.sdk.language", "cpp", cur_path)

    # Assert the resourceSpan[0].scopeSpans structure.
    scope_spans = obj["resourceSpans"][0]["scopeSpans"]
    cur_path = "resourceSpans[0].scopeSpans"
    assert isinstance(scope_spans, list), "'{}' must be a list, actual: {}" \
        .format(cur_path, type(scope_spans))
    assert len(scope_spans) == 1, "'{}' must contain exactly one element, actual: {}" \
        .format(cur_path, len(scope_spans))

    # Assert the resourceSpan[0].scopeSpans.scope object structure and contents.
    scope = scope_spans[0]["scope"]
    cur_path += "[0].scope"
    assert isinstance(scope, dict), "'{}' must be a dict, actual: {}" \
        .format(cur_path, type(scope))
    assert scope.get("name") == "org.apache.impala.impalad", "'{}.name' expected: " \
        "'org.apache.impala.impalad', actual: {}".format(cur_path, scope.get("name"))
    assert scope.get("version") == IMPALA_LOCAL_BUILD_VERSION, "'{}.version' expected: " \
        "'{}', actual: {}" \
        .format(cur_path, IMPALA_LOCAL_BUILD_VERSION, scope.get("version"))

    # Assert the resourceSpan[0].scopeSpans[0].spans structure.
    spans = scope_spans[0]["spans"]
    cur_path = "resourceSpan[0].scopeSpans[0].spans"
    assert isinstance(spans, list), "'{}' must be a list, actual: {}" \
        .format(cur_path, type(spans))
    assert len(spans) == self.ROOT_SPAN_IDX + 1, "'{}' elements count expected: " \
        "{}, actual: {}".format(cur_path, self.ROOT_SPAN_IDX + 1, len(spans))

    #
    # Assert root span. Will be the last span in the list of spans.
    #
    root_attrs = self.assert_span_common(spans, True, "root", self.ROOT_SPAN_IDX, \
        query_id, root_span_id, self.ROOT_SPAN_ATTRS_COUNT)
    assert_attr(root_attrs, "ClusterId", "otel_trace", cur_path)
    assert_attr(root_attrs, "OriginalQueryId", "", cur_path)
    assert_attr(root_attrs, "QueryId", query_id, cur_path)
    assert_attr(root_attrs, "RequestPool", "default-pool", cur_path)
    assert_attr(root_attrs, "SessionId", parse_session_id(result.runtime_profile),
        cur_path)
    assert_attr(root_attrs, "UserName", parse_db_user(result.runtime_profile), cur_path)
    assert_attr(root_attrs, "State", "FINISHED", cur_path)

    #
    # Assert Init span.
    #
    init_attrs = self.assert_span_common(spans, False, "Init", self.INIT_SPAN_IDX, \
        query_id, root_span_id, self.INIT_SPAN_ATTRS_COUNT)
    assert_attr(init_attrs, "ClusterId", "otel_trace", cur_path)
    assert_attr(init_attrs, "DefaultDb", "default", cur_path)
    assert_attr(init_attrs, "OriginalQueryId", "", cur_path)
    assert_attr(init_attrs, "QueryId", query_id, cur_path)
    assert_attr(init_attrs, "QueryString", query, cur_path)
    assert_attr(init_attrs, "RequestPool", "default-pool", cur_path)
    assert_attr(init_attrs, "SessionId", parse_session_id(result.runtime_profile),
        cur_path)
    assert_attr(init_attrs, "UserName", parse_db_user(result.runtime_profile), cur_path)

    #
    # Assert Submitted span.
    #
    self.assert_span_common(spans, False, "Submitted", self.SUBMITTED_SPAN_IDX, \
        query_id, root_span_id, self.SUBMITTED_SPAN_ATTRS_COUNT)

    #
    # Assert Planning span.
    #
    plan_attrs = self.assert_span_common(spans, False, "Planning", \
        self.PLANNING_SPAN_IDX, query_id, root_span_id, self.PLANNING_SPAN_ATTRS_COUNT)
    assert_attr(plan_attrs, "QueryType", "QUERY", cur_path)

#   @CustomClusterTestSuite.with_args(
#       impalad_args="-v=2 --cluster_id=otel_trace --otel_trace_enabled=true "
#                    "--otel_trace_exporter=file --otel_file_flush_interval_ms=500 "
#                    "--otel_file_pattern={out_dir}/" + TRACE_FILE,
#       cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
#   def test_dml_createdb(self):
#     db_name = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(7))
#     query = "create database {}".format(db_name)

#     try:
#       result = self.execute_query_expect_success(self.client, query)
#       assert result.success
#     finally:
#       assert self.execute_query_expect_success(self.client,
#           "drop database if exists {}".format(db_name))
#     query_id = result.query_id

#     # Wait until all spans are written to the trace file.
#     obj = self.__wait_for_all_spans()
#     print("Obj: " + obj)

#     # Read the root span id from the Impalad logs.
#     root_span_id, _ = self.find_span_log("root", query_id)
#     print("Root span id: {}".format(root_span_id))
#     import pdb; pdb.set_trace()

#   @CustomClusterTestSuite.with_args(
#       impalad_args="-v=2 --cluster_id=otel_trace --otel_trace_enabled=true "
#                    "--otel_trace_exporter=file --otel_file_flush_interval_ms=500 "
#                    "--otel_file_pattern={out_dir}/" + TRACE_FILE,
#       cluster_size=1, tmp_dir_placeholders=[OUT_DIR], disable_log_buffering=True)
#   def test_dml_createtable(self, unique_database):
#     query = "create database {}.footable".format(unique_database)
#     result = self.execute_query_expect_success(self.client, query)
#     assert result.success
#     query_id = result.query_id

#     # Wait until all spans are written to the trace file.
#     obj = self.__wait_for_all_spans()
#     print("Obj: " + obj)

#     # Read the root span id from the Impalad logs.
#     root_span_id, _ = self.find_span_log("root", query_id)

#     print("Root span id: {}".format(root_span_id))
#     import pdb; pdb.set_trace()

  def assert_span_common(self, spans, is_root, name, span_idx, query_id, root_span_id,
        attributes_count):
    """
       Helper function to assert common data points of a single span. Assertions include
       the span object's structure, span properties, and common span attributes.
         - spans: The list of spans from the trace file parsed into a Python object.
         - is_root: Whether the span is a root span.
         - name: The name of the span to assert without the query_id prefix.
         - span_idx: The index of the span in the spans list.
         - query_id: The query id of the span.
         - root_span_id: The root span id of the span.
         - attributes_count: The expected number of attributes in the span.
    """
    span = spans[span_idx]
    cur_path = "resourceSpan[0].scopeSpans[0].spans[{}]".format(self.INIT_SPAN_IDX)
    assert isinstance(span, dict), "{} span '{}' must be a dict, " \
        "actual: {}".format(name, cur_path, type(span))

    # Read the span trace id and span id from the Imp alad logs.
    submitted_span_id, trace_id = self.find_span_log(name, query_id)

    # Assert span properties.
    span_name = query_id

    if (is_root):
      assert_span_props(span, cur_path, query_id, trace_id, root_span_id, None, 2)
    else:
      span_name += " - {}".format(name)
      assert_span_props(span, cur_path, span_name, trace_id, submitted_span_id, \
        root_span_id)

    # Assert span attributes.
    attrs = span.get("attributes")
    cur_path += ".attributes"
    assert isinstance(attrs, list), "'{}' must be a list, actual: {}" \
        .format(cur_path, type(attrs))
    assert len(attrs) == attributes_count, "'{}' must contain " \
        "exactly {} elements, actual: {}" \
        .format(cur_path, attributes_count, len(attrs))

    if (is_root):
      assert_attr(attrs, "ErrorMessage", "TODO", cur_path)
    else:
      assert_attr(attrs, "ErrorMsg", "TODO", cur_path)
      assert_attr(attrs, "Name", span_name, cur_path)
      assert_attr(attrs, "Running", name == "Query Execution", cur_path, "boolValue")
      assert_attr(attrs, "StatusMessage", "OK", cur_path)

    assert_attr(attrs, "Status", "TODO", cur_path)

    return attrs

  def find_span_log(self, span_name, query_id):
    """
       Finds the start span log entry for the given span name and query id in the Impalad
       logs. This line line contains the trace id and span id for the span which are used
       as the expected values when asserting the span properties in the trace file.
    """
    span_regex = r'Started {} span query_id="{}" trace_id="(.*?)" span_id="(.*?)"' \
        .format(span_name, query_id)
    span_log = self.assert_impalad_log_contains("INFO", span_regex)
    trace_id = span_log.group(1)
    span_id = span_log.group(2)

    return span_id, trace_id

  def __wait_for_all_spans(self):
    """Wait until all spans are written to the trace file."""
    trace_file_path = "{}/{}".format(self.get_tmp_dir(self.OUT_DIR), self.TRACE_FILE)
    timeout = 30
    start_time = time.time()
    while True:
      if os.path.exists(trace_file_path):
        with open(trace_file_path, "r") as f:
          try:
            line = f.readline()
            if line:
              obj = json.loads(line.strip())
              scope_spans = obj["resourceSpans"][0]["scopeSpans"][0]["spans"]
              if isinstance(scope_spans, list) and len(scope_spans) == self.ROOT_SPAN_IDX + 1:
                return obj
          except Exception:
            continue
      if time.time() - start_time > timeout:
        raise RuntimeError("Timed out waiting for trace file to contain "+
          str(self.ROOT_SPAN_IDX+1) + " scopeSpans. " \
          "actual size: {}".format(len(scope_spans)))
      time.sleep(0.5)

def assert_attr(attributes, expected_key, expected_value, cur_path, type="stringValue"):
  """
     Helper function to assert that a specific OpenTelemetry attribute exists in an
     attributes list.

     An attribute is a dict with a 'key' and 'value' field. The 'key' field is a string
     containing the attribute's name. The 'value' field is a dict that contains one field
     with a name that indicates the type of the value and the field's value contains the
     actual value.

     For Example:
     {
       "key": "QueryId",
       "value": {
         "stringValue": "cc4250bd91839527:d08961da00000000"
       }
     }
  """

  assert type in ("stringValue", "boolValue", "intValue"), "Invalid type '{}', must be "\
      "one of 'stringValue', 'boolValue', or 'intValue'".format(type)

  attr_obj = next((attr for attr in attributes if attr.get("key") == expected_key), None)
  assert attr_obj is not None, "'{}' attribute not found in path '{}', actual " \
      "attributes: {}".format(expected_key, cur_path, attributes)

  attr_obj_val = attr_obj.get("value")
  assert attr_obj_val is not None, "'{}' attribute in path '{}' does not have 'value', " \
      "attribute object: {}".format(expected_key, cur_path, attr_obj)

  actual_val = attr_obj_val.get(type)
  assert actual_val == expected_value, "'{}' attribute in path '{}' expected: '{}', " \
      "actual: '{}', attribute object: {}" \
      .format(expected_key, cur_path, expected_value, actual_val, attr_obj_val)


def assert_span_props(span, cur_path, expected_name, expected_trace_id, expected_span_id,
    expected_parent_span_id, expected_kind=1):
  """Helper function to assert that a span has the expected properties. Span properties
     are at the same level as 'attributes' and include name, parentSpanId, spanId,
     traceId, flags, and kind.  Root spans do not have a parentSpanId.
  """

  actual = span.get("name")
  assert actual == expected_name, "'{}.name' expected: '{}', actual: '{}'" \
      .format(cur_path, expected_name, actual)

  actual = span.get("traceId")
  assert actual == expected_trace_id, "'{}.traceId' on span '{}' expected: '{}', " \
      "actual: '{}'".format(cur_path, expected_name, expected_trace_id, actual)

  actual = span.get("spanId")
  assert actual == expected_span_id, "'{}.spanId' on span '{}' expected: '{}', " \
      "actual: '{}'".format(cur_path, expected_name, expected_trace_id, actual)

  actual = span.get("parentSpanId")
  assert actual == expected_parent_span_id, "'{}.parentSpanId' on span '{}' expected: " \
      "'{}', actual: '{}'".format(cur_path, expected_name, expected_trace_id, actual)

  # Flags must always be 1 which indicates the trace is to be sampled.
  expected_flags = 1
  actual = span.get("flags")
  assert actual == expected_flags, "'{}.flags' on span '{}' expected: '{}', " \
      "actual: '{}'".format(cur_path, expected_name, expected_trace_id, actual)

  actual = span.get("kind")
  assert actual == expected_kind, "'{}.kind' on span '{}' expected: '{}', " \
      "actual: '{}'".format(cur_path, expected_name, expected_trace_id, actual)