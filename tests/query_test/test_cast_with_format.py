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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_client_protocol_dimension


class TestCastWithFormat(ImpalaTestSuite):

  # Run the basic tests once for Beeaswax and once for HS2. The underlying functionality
  # is independent of the file format so make sense to pick one format for testing.
  @classmethod
  def add_test_dimensions(cls):
    super(TestCastWithFormat, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())

  def test_basic_inputs_from_table(self, vector):
    self.run_test_case('QueryTest/cast_format_from_table', vector)

  def test_basic_inputs_without_row(self, vector):
    # Cast without format clause to cover the default format
    result = self.client.execute("select cast('2017-05-01 01:23:45.678912345' as "
        "timestamp)")
    assert result.data == ["2017-05-01 01:23:45.678912345"]

    # Basic input to cover a datetime with timezone scenario
    result = self.client.execute("select cast('2017-05-03 08:59:01.123456789PM 01:30'"
        "as timestamp FORMAT 'YYYY-MM-DD HH12:MI:SS.FF9PM TZH:TZM')")
    assert result.data == ["2017-05-03 20:59:01.123456789"]

    # Input that contains shuffled date without time
    result = self.client.execute("select cast('12-2010-05' as timestamp format "
        "'DD-YYYY-MM')")
    assert result.data == ["2010-05-12 00:00:00"]

    # Shuffle the input timestamp and the format clause
    result = self.client.execute("select cast('59 04-30-2017-05 01PM 01:08.123456789'"
        "as timestamp FORMAT 'MI DD-TZM-YYYY-MM TZHPM SS:HH12.FF9')")
    assert result.data == ["2017-05-04 20:59:01.123456789"]

    # Input and format without separators
    # Note, 12:01 HH12 AM is 00:01 with the internal 0-23 representation.
    result = self.client.execute("select cast('20170501120159123456789AM-0130' as "
        "timestamp FORMAT 'YYYYDDMMHH12MISSFFAMTZHTZM')")
    assert result.data == ["2017-01-05 00:01:59.123456789"]

    # Shuffled input without separators
    result = self.client.execute("select cast('59043020170501PM0108123456789'"
        "as timestamp FORMAT 'MIDDTZMYYYYMMTZHPMSSHH12FF9')")
    assert result.data == ["2017-05-04 20:59:01.123456789"]

    # Separator section lengths differ between input and format
    result = self.client.execute("select cast('--2017----05-01-' as "
        "timestamp FORMAT '-YYYY--MM---DD---')")
    assert result.data == ["2017-05-01 00:00:00"]

    # Loose separator type matching. Checking if the input/format is surrounded by
    # either single or double quotes.
    result = self.client.execute(r'''select cast("2017-./,';: 06-01" as '''
        r'''timestamp FORMAT "YYYY', -MM;:.DD")''')
    assert result.data == ["2017-06-01 00:00:00"]

    result = self.client.execute(r'''select cast('2017-./,\';: 07-01' as '''
        r'''timestamp FORMAT "YYYY', -MM;:.DD")''')
    assert result.data == ["2017-07-01 00:00:00"]

    result = self.client.execute(r'''select cast("2017-./,';: 08-01" as '''
        r'''timestamp FORMAT 'YYYY\', -MM;:.DD')''')
    assert result.data == ["2017-08-01 00:00:00"]

    result = self.client.execute(r'''select cast('2017-./,\';: 09-01' as '''
        r'''timestamp FORMAT 'YYYY\', -MM;:.DD')''')
    assert result.data == ["2017-09-01 00:00:00"]

    # Escaped double quotes in the input are not taken as the escaping character for the
    # following single quote.
    result = self.client.execute(r'''select cast("2013\\'09-01" as '''
        r'''timestamp FORMAT "YYYY'MM-DD")''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast("2013\\\'09-02" as '''
        r'''timestamp FORMAT "YYYY'MM-DD")''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast("2013\\\\'09-03" as '''
        r'''timestamp FORMAT "YYYY'MM-DD")''')
    assert result.data == ["NULL"]

    # If the input string has unprocessed tokens
    result = self.client.execute("select cast('2017-05-01 12:30' as "
        "timestamp FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]
    result = self.client.execute("select cast('2017-05-01-12:30' as "
        "timestamp FORMAT 'YYYY-MM-DD-')")
    assert result.data == ["NULL"]

    # If the format string has unprocessed tokens
    result = self.client.execute("select cast('2017-05-01' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI')")
    assert result.data == ["NULL"]
    result = self.client.execute("select cast('2017-05-01-' as "
        "timestamp FORMAT 'YYYY-MM-DD-HH12')")
    assert result.data == ["NULL"]

    # Timestamp to string types formatting
    result = self.client.execute(
        "select cast(cast('2012-11-04 13:02:59.123456' as timestamp) "
        "as string format 'DD-MM-YYYY MI:HH12:SS A.M. FF9 DDD SSSSS HH12 HH24')")
    assert result.data == ["04-11-2012 02:01:59 P.M. 123456000 309 46979 01 13"]

    result = self.client.execute(
        "select cast(cast('2012-11-04 13:02:59.123456' as timestamp) "
        "as varchar format 'DD-MM-YYYY MI:HH12:SS A.M. FF9 DDD SSSSS HH12 HH24')")
    assert result.data == ["04-11-2012 02:01:59 P.M. 123456000 309 46979 01 13"]

    result = self.client.execute(
        "select cast(cast('2012-11-04 13:02:59.123456' as timestamp) "
        "as char(50) format 'DD-MM-YYYY MI:HH12:SS A.M. FF9 DDD SSSSS HH12 HH24')")
    assert result.data == ["04-11-2012 02:01:59 P.M. 123456000 309 46979 01 13"]

    # Cast NULL string to timestamp
    result = self.client.execute("select cast(cast(NULL as string) as timestamp "
        "FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]

    # Cast NULL timestamp to string
    result = self.client.execute("select cast(cast(NULL as timestamp) as string "
        "FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]

  def test_iso8601_format(self):
    # Basic string to timestamp scenario
    result = self.client.execute("select cast('2018-11-10T15:11:04Z' as "
        "timestamp FORMAT 'YYYY-MM-DDTHH24:MI:SSZ')")
    assert result.data == ["2018-11-10 15:11:04"]

    # ISO 8601 format elements are case-insensitive
    result = self.client.execute("select cast('2018-11-09t15:11:04Z' as "
        "timestamp FORMAT 'YYYY-MM-DDTHH24:MI:SSz')")
    assert result.data == ["2018-11-09 15:11:04"]

    result = self.client.execute("select cast('2018-11-08T15:11:04z' as "
        "timestamp FORMAT 'YYYY-MM-DDtHH24:MI:SSZ')")
    assert result.data == ["2018-11-08 15:11:04"]

    # Format path
    result = self.client.execute("select cast(cast('2018-11-10 15:11:04' as "
        "timestamp) as string format 'YYYY-MM-DDTHH24:MI:SSZ')")
    assert result.data == ["2018-11-10T15:11:04Z"]

  def test_lowercase_format_elements(self):
    result = self.client.execute("select cast('2019-11-20 15:59:44.123456789 01:01' as "
        "timestamp format 'yyyy-mm-dd hh24:mi:ss.ff9 tzh-tzm')")
    assert result.data == ["2019-11-20 15:59:44.123456789"]

    result = self.client.execute("select cast('2019-300 15:59:44.123456789 01:01' as "
        "timestamp format 'yyyy-ddd hh24:mi:ss.ff9 tzh-tzm')")
    assert result.data == ["2019-10-27 15:59:44.123456789"]

    result = self.client.execute("select cast('2019-11-21 11:59:44.123456789 p.m. 01:01' "
        "as timestamp format 'yyyy-mm-dd hh12:mi:ss.ff9 am tzh-tzm')")
    assert result.data == ["2019-11-21 23:59:44.123456789"]

    result = self.client.execute("select cast('2019-11-22 10000.123456789 02:02' "
        "as timestamp format 'yyyy-mm-dd sssss ff9 tzh-tzm')")
    assert result.data == ["2019-11-22 02:46:40.123456789"]

  def test_year(self):
    # Test lower boundary of year
    result = self.client.execute("select cast('1399-05-01' as "
        "timestamp FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]

    # YYYY with less than 4 digits in the input
    query_options = dict({'now_string': '2019-01-01 11:11:11'})

    result = self.execute_query("select cast('095-01-31' as "
        "timestamp FORMAT 'YYYY-MM-DD')", query_options)
    assert result.data == ["2095-01-31 00:00:00"]

    result = self.execute_query("select cast('95-02-28' as "
        "timestamp FORMAT 'YYYY-MM-DD')", query_options)
    assert result.data == ["2095-02-28 00:00:00"]

    result = self.execute_query("select cast('5-03-31' as "
        "timestamp FORMAT 'YYYY-MM-DD')", query_options)
    assert result.data == ["2015-03-31 00:00:00"]

    # YYY with less than 3 digits in the input
    result = self.execute_query("select cast('95-04-30' as "
        "timestamp FORMAT 'YYY-MM-DD')", query_options)
    assert result.data == ["2095-04-30 00:00:00"]

    result = self.execute_query("select cast('5-05-31' as "
        "timestamp FORMAT 'YYY-MM-DD')", query_options)
    assert result.data == ["2015-05-31 00:00:00"]

    # YY with 1 digits in the input
    result = self.execute_query("select cast('5-06-30' as "
        "timestamp FORMAT 'YY-MM-DD')", query_options)
    assert result.data == ["2015-06-30 00:00:00"]

    # YYY, YY, Y tokens without separators
    result = self.execute_query("select cast('0950731' as "
        "timestamp FORMAT 'YYYMMDD')", query_options)
    assert result.data == ["2095-07-31 00:00:00"]

    result = self.execute_query("select cast('950831' as "
        "timestamp FORMAT 'YYMMDD')", query_options)
    assert result.data == ["2095-08-31 00:00:00"]

    result = self.execute_query("select cast('50930' as "
        "timestamp FORMAT 'YMMDD')", query_options)
    assert result.data == ["2015-09-30 00:00:00"]

    # Timestamp to string formatting
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'YYYY')", query_options)
    assert result.data == ["2019"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'YYY')", query_options)
    assert result.data == ["019"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'YY')", query_options)
    assert result.data == ["19"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'Y')", query_options)
    assert result.data == ["9"]

  def test_round_year(self):
    query_options = dict({'now_string': '2019-01-01 11:11:11'})

    # Test lower boundar of round year
    result = self.client.execute("select cast('1399-05-01' as "
        "timestamp FORMAT 'RRRR-MM-DD')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('1400-05-21' as "
        "timestamp FORMAT 'RRRR-MM-DD')")
    assert result.data == ["1400-05-21 00:00:00"]

    # RRRR with 4-digit year falls back to YYYY
    result = self.execute_query("select cast('2017-05-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2017-05-31 00:00:00"]

    # RRRR with 3-digit year fills digits from current year
    result = self.execute_query("select cast('017-01-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2017-01-31 00:00:00"]

    # RRRR wit 1-digit year fills digits from current year
    result = self.execute_query("select cast('0-07-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2010-07-31 00:00:00"]

    # RR with 1-digit year fills digits from current year
    result = self.execute_query("select cast('9-08-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2019-08-31 00:00:00"]

    # Round year when last 2 digits of current year is less than 50
    query_options = dict({'now_string': '2049-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2049-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["1950-03-31 00:00:00"]

    query_options = dict({'now_string': '2000-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2049-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["1950-03-31 00:00:00"]

    # Round year when last 2 digits of current year is greater than 49
    query_options = dict({'now_string': '2050-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2149-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2050-03-31 00:00:00"]

    query_options = dict({'now_string': '2099-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2149-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2050-03-31 00:00:00"]

    # In a datetime to sting cast round year act like regular 'YYYY' or 'YY' tokens.
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'RRRR')", query_options)
    assert result.data == ["2019"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'RR')", query_options)
    assert result.data == ["19"]

  def test_month_name(self):
    # Test different lowercase vs uppercase scenarios with the string to datetime path.
    result = self.execute_query("select cast('2010-February-11' as timestamp FORMAT "
        "'YYYY-MONTH-DD')")
    assert result.data == ["2010-02-11 00:00:00"]

    result = self.execute_query("select cast('2010-march-12' as timestamp FORMAT "
        "'YYYY-MONTH-DD')")
    assert result.data == ["2010-03-12 00:00:00"]

    result = self.execute_query("select cast('APRIL 13 2010' as date FORMAT "
        "'MONTH DD YYYY')")
    assert result.data == ["2010-04-13"]

    result = self.execute_query("select cast('2010 14 MAY' as timestamp FORMAT "
        "'YYYY DD MONTH')")
    assert result.data == ["2010-05-14 00:00:00"]

    result = self.execute_query("select cast('2010 14 June' as timestamp FORMAT "
        "'YYYY DD MONTH')")
    assert result.data == ["2010-06-14 00:00:00"]

    result = self.execute_query("select cast('2010 14 july' as timestamp FORMAT "
        "'YYYY DD MONTH')")
    assert result.data == ["2010-07-14 00:00:00"]

    result = self.execute_query("select cast('2010 14 AUGUST' as timestamp FORMAT "
        "'YYYY DD MONTH')")
    assert result.data == ["2010-08-14 00:00:00"]

    result = self.execute_query("select cast('2010 14 September' as date FORMAT "
        "'YYYY DD month')")
    assert result.data == ["2010-09-14"]

    result = self.execute_query("select cast('2010 14 october' as date FORMAT "
        "'YYYY DD month')")
    assert result.data == ["2010-10-14"]

    result = self.execute_query("select cast('2010 14 NOVEMBER' as date FORMAT "
        "'YYYY DD month')")
    assert result.data == ["2010-11-14"]

    result = self.execute_query("select cast('2010 14 December' as date FORMAT "
        "'YYYY DD month')")
    assert result.data == ["2010-12-14"]

    result = self.execute_query("select cast('2010 14 january' as date FORMAT "
        "'YYYY DD month')")
    assert result.data == ["2010-01-14"]

    # Test different lowercase vs uppercase scenarios with the datetime to string path.
    result = self.execute_query("select cast(date'2010-10-18' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["OCTOBER   October   october  "]

    result = self.execute_query("select cast(cast('2010-11-18' as timestamp) as string "
        "FORMAT 'MONTH Month month')")
    assert result.data == ["NOVEMBER  November  november "]

    result = self.execute_query("select cast(date'2010-12-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["DECEMBER  December  december "]

    result = self.execute_query("select cast(date'2010-01-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["JANUARY   January   january  "]

    result = self.execute_query("select cast(date'2010-02-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["FEBRUARY  February  february "]

    result = self.execute_query("select cast(date'2010-03-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["MARCH     March     march    "]

    result = self.execute_query("select cast(date'2010-04-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["APRIL     April     april    "]

    result = self.execute_query("select cast(date'2010-05-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["MAY       May       may      "]

    result = self.execute_query("select cast(date'2010-06-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["JUNE      June      june     "]

    result = self.execute_query("select cast(date'2010-07-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["JULY      July      july     "]

    result = self.execute_query("select cast(date'2010-08-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["AUGUST    August    august   "]

    result = self.execute_query("select cast(date'2010-09-19' as string FORMAT "
        "'MONTH Month month')")
    assert result.data == ["SEPTEMBER September september"]

    # Test odd casing of month token.
    result = self.execute_query("select cast(date'2010-09-20' as string FORMAT "
        "'MOnth MONth MONTh')")
    assert result.data == ["SEPTEMBER SEPTEMBER SEPTEMBER"]

    result = self.execute_query("select cast(date'2010-09-21' as string FORMAT "
        "'montH monTH moNTH moNTH')")
    assert result.data == ["september september september september"]

    # Test different lowercase vs uppercase scenarios with the datetime to string path
    # when FM is provided.
    result = self.execute_query("select cast(date'2010-10-18' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["OCTOBER October october"]

    result = self.execute_query("select cast(cast('2010-11-18' as timestamp) as string "
        "FORMAT 'FMMONTH FMMonth FMmonth')")
    assert result.data == ["NOVEMBER November november"]

    result = self.execute_query("select cast(date'2010-12-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["DECEMBER December december"]

    result = self.execute_query("select cast(date'2010-01-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["JANUARY January january"]

    result = self.execute_query("select cast(date'2010-02-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["FEBRUARY February february"]

    result = self.execute_query("select cast(date'2010-03-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["MARCH March march"]

    result = self.execute_query("select cast(date'2010-04-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["APRIL April april"]

    result = self.execute_query("select cast(date'2010-05-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["MAY May may"]

    result = self.execute_query("select cast(date'2010-06-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["JUNE June june"]

    result = self.execute_query("select cast(date'2010-07-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["JULY July july"]

    result = self.execute_query("select cast(date'2010-08-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["AUGUST August august"]

    result = self.execute_query("select cast(date'2010-09-19' as string FORMAT "
        "'FMMONTH FMMonth FMmonth')")
    assert result.data == ["SEPTEMBER September september"]

    # Incorrect month name.
    result = self.execute_query("select cast('2010 15 JU' as timestamp FORMAT "
        "'YYYY DD MONTH')")
    assert result.data == ["NULL"]

    # MONTH token without surrounding separators.
    result = self.execute_query("select cast('2010SEPTEMBER17' as date FORMAT "
        "'YYYYMONTHDD')")
    assert result.data == ["2010-09-17"]

    result = self.execute_query("select cast('2010OCTOBER17' as timestamp FORMAT "
        "'YYYYMONTHDD')")
    assert result.data == ["2010-10-17 00:00:00"]

    # Applying FX and FM modifiers on Month token.
    result = self.execute_query("select cast(cast('2010-07-20' as timestamp) as string "
        "FORMAT 'YYYYmonthDD')")
    assert result.data == ["2010july     20"]

    result = self.execute_query("select cast(date'2010-09-20' as string "
        "FORMAT 'YYYYmonthDD')")
    assert result.data == ["2010september20"]

    result = self.execute_query("select cast(cast('2010-08-20' as timestamp) as string "
        "FORMAT 'YYYYFMMonthDD')")
    assert result.data == ["2010August20"]

    result = self.execute_query("select cast(cast('2010-10-20' as timestamp) as string "
        "FORMAT 'FXYYYYFMMONTHDD')")
    assert result.data == ["2010OCTOBER20"]

    result = self.execute_query("select cast('2010-February-19' as timestamp FORMAT "
        "'FXYYYY-MONTH-DD')")
    assert result.data == ["NULL"]

    result = self.execute_query("select cast('2010-February -21' as timestamp FORMAT "
        "'FXYYYY-MONTH-DD')")
    assert result.data == ["2010-02-21 00:00:00"]

    result = self.execute_query("select cast('2010-February 22' as date FORMAT "
        "'FXYYYY-MONTHDD')")
    assert result.data == ["2010-02-22"]

    result = self.execute_query("select cast('2010-February-20' as timestamp FORMAT "
        "'FXYYYY-FMMONTH-DD')")
    assert result.data == ["2010-02-20 00:00:00"]

  def test_short_month_name(self):
    # Test different lowercase vs uppercase scenarios with the string to datetime path.
    result = self.execute_query("select cast('2015-Feb-11' as timestamp FORMAT "
        "'YYYY-MON-DD')")
    assert result.data == ["2015-02-11 00:00:00"]

    result = self.execute_query("select cast('2015-mar-12' as timestamp FORMAT "
        "'YYYY-MON-DD')")
    assert result.data == ["2015-03-12 00:00:00"]

    result = self.execute_query("select cast('APR 13 2015' as timestamp FORMAT "
        "'MON DD YYYY')")
    assert result.data == ["2015-04-13 00:00:00"]

    result = self.execute_query("select cast('2015 14 MAY' as timestamp FORMAT "
        "'YYYY DD MON')")
    assert result.data == ["2015-05-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 jun' as timestamp FORMAT "
        "'YYYY DD MON')")
    assert result.data == ["2015-06-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 Jul' as timestamp FORMAT "
        "'YYYY DD MON')")
    assert result.data == ["2015-07-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 AUG' as timestamp FORMAT "
        "'YYYY DD MON')")
    assert result.data == ["2015-08-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 Sep' as timestamp FORMAT "
        "'YYYY DD mon')")
    assert result.data == ["2015-09-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 oct' as timestamp FORMAT "
        "'YYYY DD mon')")
    assert result.data == ["2015-10-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 nov' as timestamp FORMAT "
        "'YYYY DD mon')")
    assert result.data == ["2015-11-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 DEC' as timestamp FORMAT "
        "'YYYY DD mon')")
    assert result.data == ["2015-12-14 00:00:00"]

    result = self.execute_query("select cast('2015 14 Jan' as timestamp FORMAT "
        "'YYYY DD mon')")
    assert result.data == ["2015-01-14 00:00:00"]

    # Test different lowercase vs uppercase scenarios with the datetime to string path.
    result = self.execute_query("select cast(date'2015-10-18' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["OCT Oct oct"]

    result = self.execute_query("select cast(cast('2015-11-18' as timestamp) as string "
        "FORMAT 'MON Mon mon')")
    assert result.data == ["NOV Nov nov"]

    result = self.execute_query("select cast(date'2015-12-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["DEC Dec dec"]

    result = self.execute_query("select cast(date'2015-01-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["JAN Jan jan"]

    result = self.execute_query("select cast(date'2015-02-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["FEB Feb feb"]

    result = self.execute_query("select cast(date'2015-03-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["MAR Mar mar"]

    result = self.execute_query("select cast(date'2015-04-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["APR Apr apr"]

    result = self.execute_query("select cast(date'2015-05-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["MAY May may"]

    result = self.execute_query("select cast(date'2015-06-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["JUN Jun jun"]

    result = self.execute_query("select cast(date'2015-07-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["JUL Jul jul"]

    result = self.execute_query("select cast(date'2015-08-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["AUG Aug aug"]

    result = self.execute_query("select cast(date'2015-09-19' as string FORMAT "
        "'MON Mon mon')")
    assert result.data == ["SEP Sep sep"]

    # Test odd casing of short month token.
    result = self.execute_query("select cast(date'2010-09-22' as string FORMAT "
        "'MOn mON moN')")
    assert result.data == ["SEP sep sep"]

    # Incorrect month name.
    result = self.execute_query("select cast('2015 15 JU' as timestamp FORMAT "
        "'YYYY DD MON')")
    assert result.data == ["NULL"]

    # MON token without separators in the format.
    result = self.execute_query("select cast('2015AUG17' as date FORMAT "
        "'YYYYMONDD')")
    assert result.data == ["2015-08-17"]

    result = self.execute_query("select cast(cast('2015-07-20' as timestamp) as string "
        "FORMAT 'YYYYmonDD')")
    assert result.data == ["2015jul20"]

    # FX/FM has no effect on MON.
    result = self.execute_query("select cast(cast('2015-08-21' as timestamp) as string "
        "FORMAT 'FXYYYYmonDD')")
    assert result.data == ["2015aug21"]

    result = self.execute_query("select cast(date'2015-09-22' as string "
        "FORMAT 'FXYYYYFMMonDD')")
    assert result.data == ["2015Sep22"]

  def test_week_of_year(self):
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "FORMAT 'WW')")
    assert result.data == ["01"]

    result = self.execute_query("select cast(date'2019-01-07' as string "
        "FORMAT 'WW')")
    assert result.data == ["01"]

    result = self.execute_query("select cast(cast('2019-01-08' as timestamp) as string "
        "FORMAT 'WW')")
    assert result.data == ["02"]

    result = self.execute_query("select cast(date'2019-02-01' as string "
        "FORMAT 'WW')")
    assert result.data == ["05"]

    result = self.execute_query("select cast(cast('2019-02-05' as timestamp) as string "
        "FORMAT 'WW')")
    assert result.data == ["06"]

    result = self.execute_query("select cast(date'2019-12-01' as string "
        "FORMAT 'WW')")
    assert result.data == ["48"]

    result = self.execute_query("select cast(cast('2019-12-02' as timestamp) as string "
        "FORMAT 'WW')")
    assert result.data == ["48"]

    result = self.execute_query("select cast(date'2019-12-03' as string "
        "FORMAT 'WW')")
    assert result.data == ["49"]

    result = self.execute_query("select cast(cast('2019-12-30' as timestamp) as string "
        "FORMAT 'WW')")
    assert result.data == ["52"]

    result = self.execute_query("select cast(date'2019-12-31' as string "
        "FORMAT 'WW')")
    assert result.data == ["53"]

    result = self.execute_query("select cast(cast('2020-01-01' as timestamp) as string "
        "FORMAT 'WW')")
    assert result.data == ["01"]

  def test_week_of_month(self):
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "FORMAT 'W')")
    assert result.data == ["1"]

    result = self.execute_query("select cast(date'2019-01-07' as string "
        "FORMAT 'W')")
    assert result.data == ["1"]

    result = self.execute_query("select cast(cast('2019-01-08' as timestamp) as string "
        "FORMAT 'W')")
    assert result.data == ["2"]

    result = self.execute_query("select cast(date'2019-01-14' as string "
        "FORMAT 'W')")
    assert result.data == ["2"]

    result = self.execute_query("select cast(cast('2019-01-15' as timestamp) as string "
        "FORMAT 'W')")
    assert result.data == ["3"]

    result = self.execute_query("select cast(date'2019-01-21' as string "
        "FORMAT 'W')")
    assert result.data == ["3"]

    result = self.execute_query("select cast(cast('2019-01-22' as timestamp) as string "
        "FORMAT 'W')")
    assert result.data == ["4"]

    result = self.execute_query("select cast(date'2019-01-28' as string "
        "FORMAT 'W')")
    assert result.data == ["4"]

    result = self.execute_query("select cast(cast('2019-01-29' as timestamp) as string "
        "FORMAT 'W')")
    assert result.data == ["5"]

    result = self.execute_query("select cast(date'2019-02-01' as string "
        "FORMAT 'W')")
    assert result.data == ["1"]

  def test_day_in_year(self):
    # Test "day in year" token in a non leap year scenario
    result = self.execute_query("select cast('2019 1' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-01-01 00:00:00"]

    result = self.execute_query("select cast('2019 31' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-01-31 00:00:00"]

    result = self.execute_query("select cast('2019 32' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-02-01 00:00:00"]

    result = self.execute_query("select cast('2019 60' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-03-01 00:00:00"]

    result = self.execute_query("select cast('2019 365' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-12-31 00:00:00"]

    result = self.execute_query("select cast('2019 366' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["NULL"]

    # Test "day in year" token in a leap year scenario
    result = self.execute_query("select cast('2000 60' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2000-02-29 00:00:00"]

    result = self.execute_query("select cast('2000 61' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2000-03-01 00:00:00"]

    result = self.execute_query("select cast('2000 366' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2000-12-31 00:00:00"]

    result = self.execute_query("select cast('2000 367' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["NULL"]

    # Test "day in year" token without separators
    result = self.execute_query("select cast('20190011120' as timestamp "
        "FORMAT 'YYYYDDDHH12MI')")
    assert result.data == ["2019-01-01 11:20:00"]

    # Timestamp to string formatting
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format'DDD')")
    assert result.data == ["001"]

    result = self.execute_query("select cast(cast('2019-12-31' as timestamp) as string "
        "format'DDD')")
    assert result.data == ["365"]

    result = self.execute_query("select cast(cast('2000-12-31' as timestamp) as string "
        "format'DDD')")
    assert result.data == ["366"]

    result = self.execute_query("select cast(cast('2019 123' as timestamp "
        "format 'YYYY DDD') as string format'DDD')")
    assert result.data == ["123"]

  def test_day_name(self):
    # String to datetime: Test different lowercase vs uppercase scenarios.
    result = self.execute_query(
        "select cast('2010-08-Tuesday' as timestamp FORMAT 'IYYY-IW-DAY'), "
        "       cast('2010-monday-08' as timestamp FORMAT 'IYYY-DAY-IW'), "
        "       cast('2010-Wednesday-08' as date FORMAT 'IYYY-DAY-IW'), "
        "       cast('2010 08 THURSDAY' as timestamp FORMAT 'IYYY IW DAY'), "
        "       cast('2010 08 Friday' as date FORMAT 'IYYY IW DAY'), "
        "       cast('2010 08 saturday' as timestamp FORMAT 'IYYY IW DAY'), "
        "       cast('sUnDay 2010 08' as date FORMAT 'DAY IYYY IW'), "
        "       cast('Monday 2010 09' as date FORMAT 'DAY IYYY IW')")
    assert result.data == [
        "2010-02-23 00:00:00\t2010-02-22 00:00:00\t2010-02-24\t"
        "2010-02-25 00:00:00\t2010-02-26\t2010-02-27 00:00:00\t"
        "2010-02-28\t2010-03-01"]
    # And now with short day names.
    result = self.execute_query(
        "select cast('2010-08-Tue' as timestamp FORMAT 'IYYY-IW-DY'), "
        "       cast('2010-mon-08' as timestamp FORMAT 'IYYY-DY-IW'), "
        "       cast('2010-Wed-08' as date FORMAT 'IYYY-DY-IW'), "
        "       cast('2010 08 THU' as timestamp FORMAT 'IYYY IW DY'), "
        "       cast('2010 08 Fri' as date FORMAT 'IYYY IW DY'), "
        "       cast('2010 08 sat' as timestamp FORMAT 'IYYY IW DY'), "
        "       cast('sUn 2010 08' as date FORMAT 'DY IYYY IW'), "
        "       cast('Mon 2010 09' as date FORMAT 'DY IYYY IW')")
    assert result.data == [
        "2010-02-23 00:00:00\t2010-02-22 00:00:00\t2010-02-24\t"
        "2010-02-25 00:00:00\t2010-02-26\t2010-02-27 00:00:00\t"
        "2010-02-28\t2010-03-01"]

    # String to datetime: Incorrect day name.
    result = self.execute_query("select cast('2010 09 Mondau' as timestamp FORMAT "
        "'IYYY IW DAY')")
    assert result.data == ["NULL"]

    # String to datetime: DAY token without surrounding separators.
    result = self.execute_query(
        "select cast('2010MONDAY09' as date FORMAT 'IYYYDAYIW'), "
        "       cast('2010WEDNESDAY9' as timestamp FORMAT 'IYYYDAYIW')")
    assert result.data == ["2010-03-01\t2010-03-03 00:00:00"]
    # And now with short day names.
    result = self.execute_query(
        "select cast('2010MON09' as date FORMAT 'IYYYDYIW'), "
        "       cast('2010WED9' as timestamp FORMAT 'IYYYDYIW')")
    assert result.data == ["2010-03-01\t2010-03-03 00:00:00"]

    # String to datetime: FX and FM modifiers.
    result = self.execute_query(
        "select cast('2010-Monday-09' as timestamp FORMAT 'FXIYYY-DAY-IW'), "
        "       cast('2010-Monday  X-09' as timestamp FORMAT 'FXIYYY-DAY-IW')")
    assert result.data == ["NULL\tNULL"]

    result = self.execute_query(
        "select cast('2010-Monday   -09' as timestamp FORMAT 'FXIYYY-DAY-IW'), "
        "       cast('2010-Monday   09' as date FORMAT 'FXIYYY-DAYIW')")
    assert result.data == ["2010-03-01 00:00:00\t2010-03-01"]

    result = self.execute_query(
        "select cast('2010-Monday-09' as timestamp FORMAT 'FXIYYY-FMDAY-IW'), "
        "       cast('2010-Monday09' as timestamp FORMAT 'FXIYYY-FMDAYIW'), "
        "       cast('2010Monday09' as date FORMAT 'FXIYYYFMDAYIW')")
    assert result.data == ["2010-03-01 00:00:00\t2010-03-01 00:00:00\t"
                           "2010-03-01"]

    # Datetime to string: Different lowercase and uppercase scenarios.
    result = self.execute_query("select cast(date'2019-11-13' as string "
        "format 'DAY Day day DY Dy dy')")
    assert result.data == ["WEDNESDAY Wednesday wednesday WED Wed wed"]

    result = self.execute_query("select cast(cast('2019-11-14' as timestamp) as string "
        "format 'DAY Day day DY Dy dy')")
    assert result.data == ["THURSDAY  Thursday  thursday  THU Thu thu"]

    result = self.execute_query("select cast(date'2019-11-15' as string "
        "format 'DAY Day day DY Dy dy')")
    assert result.data == ["FRIDAY    Friday    friday    FRI Fri fri"]

    result = self.execute_query("select cast(cast('2019-11-16' as timestamp) as string "
        "format 'DAY Day day DY Dy dy')")
    assert result.data == ["SATURDAY  Saturday  saturday  SAT Sat sat"]

    result = self.execute_query("select cast(date'2019-11-17' as string "
        "format 'DAY Day day DY Dy dy')")
    assert result.data == ["SUNDAY    Sunday    sunday    SUN Sun sun"]

    result = self.execute_query("select cast(cast('2019-11-18' as timestamp) as string "
        "format 'DAY Day day DY Dy dy')")
    assert result.data == ["MONDAY    Monday    monday    MON Mon mon"]

    result = self.execute_query("select cast(date'2019-11-19' as string "
        "format 'DAY Day day DY Dy dy')")
    assert result.data == ["TUESDAY   Tuesday   tuesday   TUE Tue tue"]

    # Datetime to string: Different lowercase and uppercase scenarios when FM is provided.
    result = self.execute_query("select cast(cast('2019-11-13' as timestamp) as string "
        "format 'FMDAY FMDay FMday FMDY FMDy FMdy')")
    assert result.data == ["WEDNESDAY Wednesday wednesday WED Wed wed"]

    result = self.execute_query("select cast(date'2019-11-14' as string "
        "format 'FMDAY FMDay FMday FMDY FMDy FMdy')")
    assert result.data == ["THURSDAY Thursday thursday THU Thu thu"]

    result = self.execute_query("select cast(cast('2019-11-15' as timestamp) as string "
        "format 'FMDAY FMDay FMday FMDY FMDy FMdy')")
    assert result.data == ["FRIDAY Friday friday FRI Fri fri"]

    result = self.execute_query("select cast(date'2019-11-16' as string "
        "format 'FMDAY FMDay FMday FMDY FMDy FMdy')")
    assert result.data == ["SATURDAY Saturday saturday SAT Sat sat"]

    result = self.execute_query("select cast(cast('2019-11-17' as timestamp) as string "
        "format 'FMDAY FMDay FMday FMDY FMDy FMdy')")
    assert result.data == ["SUNDAY Sunday sunday SUN Sun sun"]

    result = self.execute_query("select cast(date'2019-11-18' as string "
        "format 'FMDAY FMDay FMday FMDY FMDy FMdy')")
    assert result.data == ["MONDAY Monday monday MON Mon mon"]

    result = self.execute_query("select cast(cast('2019-11-19' as timestamp) as string "
        "format 'FMDAY FMDay FMday FMDY FMDy FMdy')")
    assert result.data == ["TUESDAY Tuesday tuesday TUE Tue tue"]

    # Datetime to string: Test odd casing of day token.
    result = self.execute_query("select cast(date'2010-01-20' as string FORMAT "
        "'DAy dAY daY dY')")
    assert result.data == ["WEDNESDAY wednesday wednesday wed"]

    # Datetime to string: Day token without surrounding separators.
    result = self.execute_query("select cast(date'2019-11-11' as string "
        "format 'YYYYDayMonth')")
    assert result.data == ["2019Monday   November "]

    result = self.execute_query("select cast(cast('2019-11-12' as timestamp) as string "
        "format 'YYYYDYDD')")
    assert result.data == ["2019TUE12"]

    result = self.execute_query("select cast(date'2019-11-11' as string "
        "format 'YYYYDayMonth')")
    assert result.data == ["2019Monday   November "]

    result = self.execute_query("select cast(cast('2019-11-12' as timestamp) as string "
        "format 'YYYYDYDD')")
    assert result.data == ["2019TUE12"]

    # Datetime to string: Day token with FM and FX modifiers.
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'FXYYYY DAY DD')")
    assert result.data == ["2019 TUESDAY   01"]

    result = self.execute_query("select cast(date'2019-01-01' as string "
        "format 'FXYYYY FMDAY DD')")
    assert result.data == ["2019 TUESDAY 01"]

    result = self.execute_query("select cast(cast('2019-02-02' as timestamp) as string "
        "format 'FXYYYY DY DD')")
    assert result.data == ["2019 SAT 02"]

    result = self.execute_query("select cast(date'2019-02-02' as string "
        "format 'FXYYYY FMDY DD')")
    assert result.data == ["2019 SAT 02"]

  def test_second_of_day(self):
    # Check boundaries
    result = self.client.execute("select cast('2019-11-10 86399.11' as "
        "timestamp FORMAT 'YYYY-MM-DD SSSSS.FF2')")
    assert result.data == ["2019-11-10 23:59:59.110000000"]

    result = self.client.execute("select cast('2019-11-10 0' as "
        "timestamp FORMAT 'YYYY-MM-DD SSSSS')")
    assert result.data == ["2019-11-10 00:00:00"]

    # Without separators full 5-digit "second of day" has to be given
    result = self.client.execute("select cast('11-10 036612019' as "
        "timestamp FORMAT 'MM-DD SSSSSYYYY')")
    assert result.data == ["2019-11-10 01:01:01"]

    # Check timezone offsets with "second of day"
    result = self.client.execute("select cast('2019-11-10 036611010' as "
        "timestamp FORMAT 'YYYY-MM-DD SSSSSTZHTZM')")
    assert result.data == ["2019-11-10 01:01:01"]

    # Timestamp to string formatting
    result = self.client.execute("select cast(cast('2019-01-01 01:01:01' as timestamp) "
        "as string format 'SSSSS')")
    assert result.data == ["03661"]

    result = self.client.execute("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'SSSSS')")
    assert result.data == ["00000"]

    result = self.client.execute("select cast(cast('2019-01-01 23:59:59' as timestamp) "
        "as string format 'SSSSS')")
    assert result.data == ["86399"]

  def test_day_of_week(self):
    # Sunday is 1
    result = self.execute_query("select cast(cast('2019-11-03' as timestamp) as string "
        "FORMAT 'D')")
    assert result.data == ["1"]

    result = self.execute_query("select cast(cast('2019-11-03' as date) as string "
        "FORMAT 'D')")
    assert result.data == ["1"]

    # Wednesday is 4
    result = self.execute_query("select cast(cast('2019-11-06' as timestamp) as string "
        "FORMAT 'D')")
    assert result.data == ["4"]

    result = self.execute_query("select cast(cast('2019-11-06' as date) as string "
        "FORMAT 'D')")
    assert result.data == ["4"]

    # Saturday is 7
    result = self.execute_query("select cast(cast('2019-11-09' as timestamp) as string "
        "FORMAT 'D')")
    assert result.data == ["7"]

    result = self.execute_query("select cast(cast('2019-11-09' as date) as string "
        "FORMAT 'D')")
    assert result.data == ["7"]

    # FX and FM modifier does not pad day of week values with zeros.
    result = self.execute_query("select cast(cast('2019-12-01' as date) as string "
        "FORMAT 'FXD')")
    assert result.data == ["1"]

    result = self.execute_query("select cast(cast('2019-12-02' as date) as string "
        "FORMAT 'FXFMD')")
    assert result.data == ["2"]

  def test_fraction_seconds(self):
    result = self.execute_query("select cast('2019-11-08 123456789' as "
        "timestamp FORMAT 'YYYY-MM-DD FF9')")
    assert result.data == ["2019-11-08 00:00:00.123456789"]

    result = self.execute_query("select cast('2019-11-08 1' as "
        "timestamp FORMAT 'YYYY-MM-DD FF')")
    assert result.data == ["2019-11-08 00:00:00.100000000"]

    result = self.execute_query("select cast('2019-11-08 1234567890' as "
        "timestamp FORMAT 'YYYY-MM-DD FF')")
    assert result.data == ["NULL"]

    result = self.execute_query("select cast('2019-11-08' as "
        "timestamp FORMAT 'YYYY-MM-DD FF')")
    assert result.data == ["NULL"]

    self.run_fraction_test(1)
    self.run_fraction_test(2)
    self.run_fraction_test(3)
    self.run_fraction_test(4)
    self.run_fraction_test(5)
    self.run_fraction_test(6)
    self.run_fraction_test(7)
    self.run_fraction_test(8)
    self.run_fraction_test(9)

  def run_fraction_test(self, length):
    MAX_LENGTH = 9
    fraction_part = ""
    for x in range(length):
      fraction_part += str(x + 1)
    template_input = "select cast('2019-11-08 %s' as timestamp FORMAT 'YYYY-MM-DD FF%s')"
    input_str = template_input % (fraction_part, length)

    expected = "2019-11-08 00:00:00." + fraction_part + ("0" * (MAX_LENGTH - length))
    result = self.execute_query(input_str)
    assert result.data == [expected]

    input2_str = template_input % (fraction_part + str(length + 1), length)
    result = self.execute_query(input2_str)
    assert result.data == ["NULL"]

  def test_meridiem_indicator(self):
    # Check 12 hour diff between AM and PM
    result = self.client.execute("select cast('2017-05-03 08 AM' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12 AM')")
    assert result.data == ["2017-05-03 08:00:00"]

    result = self.client.execute("select cast('2017-05-04 08 PM' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12 PM')")
    assert result.data == ["2017-05-04 20:00:00"]

    # Check that any meridiem indicator in the pattern matches any meridiem indicator in
    # the input
    result = self.client.execute("select cast('2017-05-05 12AM' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12PM')")
    assert result.data == ["2017-05-05 00:00:00"]

    result = self.client.execute("select cast('2017-05-06 P.M.12' as "
        "timestamp FORMAT 'YYYY-MM-DD AMHH12')")
    assert result.data == ["2017-05-06 12:00:00"]

    result = self.client.execute("select cast('2017-05-07 PM 01' as "
        "timestamp FORMAT 'YYYY-MM-DD A.M. HH12')")
    assert result.data == ["2017-05-07 13:00:00"]

    # Test lowercase indicator in input
    result = self.client.execute("select cast('2017-05-08 pm09' as "
        "timestamp FORMAT 'YYYY-MM-DD P.M.HH12')")
    assert result.data == ["2017-05-08 21:00:00"]

    result = self.client.execute("select cast('2017-05-09 10a.m.' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12PM')")
    assert result.data == ["2017-05-09 10:00:00"]

    # Test that '.' in indicator doesn't conflict with '.' as separator
    result = self.client.execute("select cast('2017-05-11 9.AM.10' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12.P.M..MI')")
    assert result.data == ["2017-05-11 09:10:00"]

    result = self.client.execute("select cast('2017-05-10.P.M..10' as "
        "timestamp FORMAT 'YYYY-MM-DD.AM.HH12')")
    assert result.data == ["2017-05-10 22:00:00"]

    # Timestamp to string formatting
    result = self.client.execute("select cast(cast('2019-01-01 00:15:10' as timestamp) "
        "as string format 'HH12 P.M.')")
    assert result.data == ["12 A.M."]

    result = self.client.execute("select cast(cast('2019-01-01 12:15:10' as timestamp) "
        "as string format 'HH12 AM')")
    assert result.data == ["12 PM"]

    result = self.client.execute("select cast(cast('2019-01-01 13:15:10' as timestamp) "
        "as string format 'HH12 a.m.')")
    assert result.data == ["01 p.m."]

    result = self.client.execute("select cast(cast('2019-01-01 23:15:10' as timestamp) "
        "as string format 'HH12 p.m.')")
    assert result.data == ["11 p.m."]

  def test_timezone_offsets(self):
    # Test positive timezone offset.
    result = self.client.execute("select cast('2018-01-01 10:00 AM +15:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-01-01 10:00:00"]

    # Test negative timezone offset.
    result = self.client.execute("select cast('2018-12-31 08:00 PM -15:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-12-31 20:00:00"]

    # Minus sign before TZM.
    result = self.client.execute("select cast('2018-12-31 08:00 AM 01:-59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    # Minus sign right before one digit TZH.
    result = self.client.execute("select cast('2018-12-31 08:00 AM--1:10' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    result = self.client.execute("select cast('2018-12-31 08:00 AM-5:00' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M.TZH:TZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    result = self.client.execute("select cast('2018-12-31 08:00 AM-+1:10' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    # Invalid TZH and TZM
    result = self.client.execute("select cast('2016-01-01 10:00 AM +16:00' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2016-01-01 11:00 AM -16:00' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2016-01-01 10:00 AM 16:00' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2016-01-01 10:00 AM +15:60' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["NULL"]

    # One digit negative TZH at the end of the input string.
    result = self.client.execute("select cast('2018-12-31 12:01 -1' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI TZH')")
    assert result.data == ["2018-12-31 12:01:00"]

    # Test timezone offset parsing without separators
    result = self.client.execute("select cast('201812310800AM+0515' as "
        "timestamp FORMAT 'YYYYMMDDHH12MIA.M.TZHTZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    result = self.client.execute("select cast('201812310800AM0515' as "
        "timestamp FORMAT 'YYYYMMDDHH12MIA.M.TZHTZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    result = self.client.execute("select cast('201812310800AM-0515' as "
        "timestamp FORMAT 'YYYYMMDDHH12MIA.M.TZHTZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    # Test signed zero TZH with not null TZM
    result = self.client.execute("select cast('2018-01-01 10:00 AM +00:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-01-01 10:00:00"]

    result = self.client.execute("select cast('2018-01-01 10:00 AM -00:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-01-01 10:00:00"]

    # Shuffle TZH and TZM into other elements
    result = self.client.execute("select cast('2018-01-01 15 10:00 1 AM' as "
        "timestamp FORMAT 'YYYY-MM-DD TZM HH12:MI TZH A.M.')")
    assert result.data == ["2018-01-01 10:00:00"]

    result = self.client.execute("select cast('2018-01-011510:00-01AM' as "
        "timestamp FORMAT 'YYYY-MM-DDTZMHH12:MITZHA.M.')")
    assert result.data == ["2018-01-01 10:00:00"]

    # Timezone offset with default time
    result = self.client.execute("select cast('2018-01-01 01:30' as timestamp "
        "FORMAT 'YYYY-MM-DD TZH:TZM')")
    assert result.data == ["2018-01-01 00:00:00"]

    # Single minus sign before two digit TZH.
    result = self.client.execute("select cast('2018-09-11 15:30:10-10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS-TZH')")
    assert result.data == ["2018-09-11 15:30:10"]

    # Non-digit TZH and TZM.
    result = self.client.execute("select cast('2018-09-11 17:30:10 ab:10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2018-09-11 17:30:10 -ab:10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2018-09-11 17:30:10 +ab:10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2018-09-11 18:30:10 10:ab' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

  def test_text_token(self):
    # Parse ISO:8601 tokens using the text token.
    result = self.client.execute(r'''select cast('1985-11-19T01:02:03Z' as timestamp '''
        r'''format 'YYYY-MM-DD"T"HH24:MI:SS"Z"')''')
    assert result.data == ["1985-11-19 01:02:03"]

    # Free text at the end of the input
    result = self.client.execute(r'''select cast('1985-11-19text' as timestamp '''
        r'''format 'YYYY-MM-DD"text"')''')
    assert result.data == ["1985-11-19 00:00:00"]

    # Free text at the beginning of the input
    result = self.client.execute(r'''select cast('19801985-11-20' as timestamp '''
        r'''format '"1980"YYYY-MM-DD')''')
    assert result.data == ["1985-11-20 00:00:00"]

    # Empty text in format
    result = self.client.execute(r'''select cast('1985-11-21' as timestamp '''
        r'''format '""YYYY""-""MM""-""DD""')''')
    assert result.data == ["1985-11-21 00:00:00"]

    result = self.client.execute(r'''select cast('1985-11-22' as timestamp '''
        r'''format 'YYYY-MM-DD""""""')''')
    assert result.data == ["1985-11-22 00:00:00"]

    result = self.client.execute(r'''select cast('1985-12-09-' as timestamp '''
        r'''format 'YYYY-MM-DD-""')''')
    assert result.data == ["1985-12-09 00:00:00"]

    result = self.client.execute(r'''select cast('1985-12-10-' as date '''
        r'''format 'FXYYYY-MM-DD-""')''')
    assert result.data == ["1985-12-10"]

    result = self.client.execute(r'''select cast('1985-11-23' as timestamp '''
        r'''format 'YYYY-MM-DD""""""HH24')''')
    assert result.data == ["NULL"]

    # Text in input doesn't match with the text in format
    result = self.client.execute(r'''select cast('1985-11-24Z01:02:03Z' as timestamp '''
        r'''format 'YYYY-MM-DD"T"HH24:MI:SS"Z"')''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast('1985-11-24T01:02:04T' as timestamp '''
        r'''format 'YYYY-MM-DD"T"HH24:MI:SS"Z"')''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast('1985-11-2401:02:05Z' as timestamp '''
        r'''format 'YYYY-MM-DD"T"HH24:MI:SS"Z"')''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast('1985-11-24T01:02:06' as timestamp '''
        r'''format 'YYYY-MM-DD"T"HH24:MI:SS"Z"')''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast('1985-11-24 01:02:07te' as timestamp '''
        r'''format 'YYYY-MM-DD HH24:MI:SS"text"')''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast('1985-11-24 01:02:08text' as '''
        r'''timestamp format 'YYYY-MM-DD HH24:MI:SS"te"')''')
    assert result.data == ["NULL"]

    # Consecutive text tokens
    result = self.client.execute(r'''select cast('1985-11text1text2-25' as timestamp '''
        r'''format 'YYYY-MM"text1""text2"-DD')''')
    assert result.data == ["1985-11-25 00:00:00"]

    # Separators in text token
    result = self.client.execute(r'''select cast("1985-11 -'./,:-25" as date '''
        r'''format "YYYY-MM\" -'./,:\"-DD")''')
    assert result.data == ["1985-11-25"]

    # Known limitation: If a text token containing separator characters at the beginning
    # is right after a separator token sequence then parsing can't find where to stop when
    # parsing the consecutive separators. Use FX modifier in this case for strict
    # matching.
    result = self.client.execute(r'''select cast("1986-11'25" as date '''
        r'''format "YYYY-MM\"'\"DD")''')
    assert result.data == ["1986-11-25"]

    result = self.client.execute(r'''select cast("1986-11-'25" as timestamp '''
        r'''format "YYYY-MM-\"'\"DD")''')
    assert result.data == ["NULL"]

    result = self.client.execute(r'''select cast("1986-10-'25" as timestamp '''
        r'''format "FXYYYY-MM-\"'\"DD")''')
    assert result.data == ["1986-10-25 00:00:00"]

    # Escaped quotation mark is in the text token.
    result = self.client.execute(r'''select cast('1985-11a"b26' as timestamp '''
        r'''format 'YYYY-MM"a\"b"DD')''')
    assert result.data == ["1985-11-26 00:00:00"]

    # Format part is surrounded by double quotes so the quotes indicating the start and
    # end of the text token has to be escaped.
    result = self.client.execute('select cast("year: 1985, month: 11, day: 27" as date'
        r''' format "\"year: \"YYYY\", month: \"MM\", day: \"DD")''')
    assert result.data == ["1985-11-27"]

    # Scenario when there is an escaped double quote inside a text token that is itself
    # surrounded by escaped double quotes.
    result = self.client.execute(r'''select cast("1985 some \"text 11-28" as date'''
        r''' format "YYYY\" some \\\"text \"MM-DD")''')
    assert result.data == ["1985-11-28"]

    # When format is surrounded by single quotes and there is a single quote inside the
    # text token that has to be escaped.
    result = self.client.execute(r'''select cast("1985 some 'text 11-29" as date'''
        r''' format 'YYYY" some \'text "MM-DD')''')
    assert result.data == ["1985-11-29"]
    result = self.client.execute(r'''select cast("1985 some 'text 11-29" as timestamp'''
        r''' format 'YYYY" some \'text "MM-DD')''')
    assert result.data == ["1985-11-29 00:00:00"]

    # Datetime to string path: Simple text token.
    result = self.client.execute(r'''select cast(cast("1985-11-30" as date) as string '''
        r'''format "YYYY-\"text\"MM-DD")''')
    assert result.data == ["1985-text11-30"]

    # Datetime to string path: Consecutive text tokens.
    result = self.client.execute(r'''select cast(cast("1985-12-01" as date) as string '''
        r'''format "YYYY-\"text1\"\"text2\"MM-DD")''')
    assert result.data == ["1985-text1text212-01"]
    result = self.client.execute(r'''select cast(cast("1985-12-01" as timestamp) as '''
        r'''string format "YYYY-\"text1\"\"text2\"MM-DD")''')
    assert result.data == ["1985-text1text212-01"]

    # Datetime to string path: Text token containing separators.
    result = self.client.execute(r'''select cast(cast("1985-12-02" as date) as '''
        r'''string format "YYYY-\" -'./,:\"MM-DD")''')
    assert result.data == ["1985- -'./,:12-02"]
    result = self.client.execute(r'''select cast(cast("1985-12-02" as timestamp) as '''
        r'''string format "YYYY-\" -'./,:\"MM-DD")''')
    assert result.data == ["1985- -'./,:12-02"]

    # Datetime to string path: Text token containing a double quote.
    result = self.client.execute(r'''select cast(cast('1985-12-03' as date) as string '''
        r'''format 'YYYY-"some \"text"MM-DD')''')
    assert result.data == ['1985-some "text12-03']
    result = self.client.execute(r'''select cast(cast('1985-12-03' as timestamp) as '''
        r'''string format 'YYYY-"some \"text"MM-DD')''')
    assert result.data == ['1985-some "text12-03']

    # Datetime to string path: Text token containing a double quote where the text token
    # itself is covered by escaped double quotes.
    result = self.client.execute(r'''select cast(cast("1985-12-04" as date) as string '''
        r'''format "YYYY-\"some \\\"text\"MM-DD")''')
    assert result.data == ['1985-some "text12-04']
    result = self.client.execute(r'''select cast(cast("1985-12-04" as timestamp) as '''
        r'''string format "YYYY-\"some \\\"text\"MM-DD")''')
    assert result.data == ['1985-some "text12-04']

    # Backslash in format that escapes non-special chars.
    result = self.client.execute(r'''select cast("1985- some \ text12-05" as date '''
        r'''format 'YYYY-"some \ text"MM-DD')''')
    assert result.data == ['1985-12-05']
    result = self.client.execute(r'''select cast(cast("1985-12-06" as date) as string '''
        r'''format 'YYYY-"some \ text"MM-DD')''')
    assert result.data == ['1985-some  text12-06']

    result = self.client.execute(r'''select cast("1985-some text12-07" as date '''
        r'''format 'YYYY-"\some text"MM-DD')''')
    assert result.data == ['1985-12-07']
    result = self.client.execute(r'''select cast(cast("1985-12-08" as date) as string '''
        r'''format 'YYYY-"\some text"MM-DD')''')
    assert result.data == ['1985-some text12-08']

    # Backslash in format that escapes special chars.
    result = self.client.execute(r'''select cast("1985-\b\n\r\t12-09" as '''
        r'''date format 'YYYY-"\b\n\r\t"MM-DD')''')
    assert result.data == ['1985-12-09']
    result = self.client.execute(r'''select cast(cast("1985-12-10" as date) as string '''
        r'''format 'YYYY"\ttext\n"MM-DD')''')
    assert result.data == [r'''1985	text
12-10''']
    result = self.client.execute(r'''select cast(cast("1985-12-11" as date) as string '''
        r'''format "YYYY\"\ttext\n\"MM-DD")''')
    assert result.data == [r'''1985	text
12-11''']
    result = self.client.execute(r'''select cast(cast("1985-12-12" as timestamp) as '''
        r'''string format 'YYYY"\ttext\n"MM-DD')''')
    assert result.data == [r'''1985	text
12-12''']
    result = self.client.execute(r'''select cast(cast("1985-12-13" as timestamp) as '''
        r'''string format "YYYY\"\ttext\n\"MM-DD")''')
    assert result.data == [r'''1985	text
12-13''']

    # Escaped backslash in text token.
    result = self.client.execute(r'''select cast(cast("1985-12-14" as date) as string '''
        r'''format 'YYYY"some\\text"MM-DD')''')
    assert result.data == [r'''1985some\text12-14''']
    result = self.client.execute(r'''select cast(cast("1985-12-15" as timestamp) as '''
        r'''string format 'YYYY"\\"MM"\\"DD')''')
    assert result.data == [r'''1985\12\15''']
    result = self.client.execute(r'''select cast("1985\\12\\14 01:12:10" as timestamp '''
        r'''format 'YYYY"\\"MM"\\"DD HH12:MI:SS')''')
    assert result.data == [r'''1985-12-14 01:12:10''']
    # Known limitation: When the format token is surrounded by escaped quotes then an
    # escaped backslash at the end of the token together with the closing double quote is
    # taken as a double escaped quote.
    err = self.execute_query_expect_failure(self.client,
        r'''select cast(cast("1985-12-16" as timestamp) as string format '''
        r'''"YYYY\"\\\"MM\"\\\"DD")''')
    assert "Bad date/time conversion format" in str(err)

    # Free text token where an escaped backslash precedes an escaped single quote.
    result = self.client.execute(r'''select cast("2010-\\'-02-01" as date format '''
        r''' 'FXYYYY-"\\\'"-MM-DD') ''')
    assert result.data == ["2010-02-01"]

    # Test error message where format contains text token with escaped double quote.
    err = self.execute_query_expect_failure(self.client,
        r'''select cast('1985-AB"CD11-23' as date format 'YYYY-"AB\"C"MM-DD')''')
    assert (r'''String to Date parse failed. Input '1985-AB"CD11-23' doesn't match '''
        r'''with format 'YYYY-"AB\"C"MM-DD''' in str(err))

  def test_iso8601_week_based_date_tokens(self):
    # Format 0001-01-01 and 9999-12-31 dates.
    # 0001-01-01 is Monday, belongs to the 1st week of year 1.
    # 9999-12-31 is Friday, belongs to the 52nd week of year 9999.
    result = self.client.execute(
        "select cast(date'0001-01-01' as string format 'IYYY/IW/ID'), "
        "       cast(date'9999-12-31' as string format 'IYYY/IW/ID')")
    assert result.data == ["0001/01/01\t9999/52/05"]

    # Parse 0001-01-01 and 9999-12-31 dates.
    result = self.client.execute(
        "select cast('0001/01/01' as date format 'IYYY/IW/ID'), "
        "       cast('9999/52/05' as date format 'IYYY/IW/ID')")
    assert result.data == ["0001-01-01\t9999-12-31"]

    # Parse out-of-range dates.
    # Year 9999 has 52 weeks. 9999-12-31 is Friday.
    err = self.execute_query_expect_failure(self.client,
        "select cast('9999/52/06' as date format 'IYYY/IW/ID')")
    assert (r'''String to Date parse failed. Input '9999/52/06' doesn't match with '''
        r'''format 'IYYY/IW/ID''' in str(err))
    err = self.execute_query_expect_failure(self.client,
        "select cast('9999/53/01' as date format 'IYYY/IW/ID')")
    assert (r'''String to Date parse failed. Input '9999/53/01' doesn't match with '''
        r'''format 'IYYY/IW/ID''' in str(err))

    # Format 1400-01-01 and 9999-12-31 timestamps.
    # 1400-01-01 is Wednesday, belongs to the 1st week of year 1400.
    # 9999-12-31 is Friday, belongs to the 52nd week of year 9999.
    result = self.client.execute(
        "select cast(cast('1400-01-01' as timestamp) as string format 'IYYY/IW/ID'), "
        "       cast(cast('9999-12-31' as timestamp) as string format 'IYYY/IW/ID')")
    assert result.data == ["1400/01/03\t9999/52/05"]

    # Parse 1400-01-01 and 9999-12-31 timestamps.
    result = self.client.execute(
        "select cast('1400/01/03' as timestamp format 'IYYY/IW/ID'), "
        "       cast('9999/52/05' as timestamp format 'IYYY/IW/ID')")
    assert result.data == ["1400-01-01 00:00:00\t9999-12-31 00:00:00"]

    # Parse out-of-range timestamps.
    # - Tuesday of the 1st week of year 1400 is 1399-12-31, which is out of the valid
    # timestamp range.
    # - Year 9999 has 52 weeks. 9999-12-31 is Friday.
    result = self.client.execute(
        "select cast('1400/01/02' as timestamp format 'IYYY/IW/ID'), "
        "       cast('9999/52/06' as timestamp format 'IYYY/IW/ID'), "
        "       cast('9999/53/01' as timestamp format 'IYYY/IW/ID')")
    assert result.data == ["NULL\tNULL\tNULL"]

    # Formatting dates arond Dec 31.
    # 2019-12-31 is Tuesday, belongs to 1st week of year 2020.
    # 2020-12-31 is Thursday, belongs to 53rd week of year 2020.
    result = self.client.execute(
        "select cast(date'2019-12-29' as string format 'IYYY/IW/ID'), "
        "       cast(date'2019-12-30' as string format 'IYYY/IW/ID'), "
        "       cast(date'2019-12-31' as string format 'IYYY/IW/ID'), "
        "       cast(date'2020-01-01' as string format 'IYYY/IW/ID'), "
        "       cast(date'2020-12-31' as string format 'IYYY/IW/ID'), "
        "       cast(date'2021-01-01' as string format 'IYYY/IW/ID')")
    assert result.data == [
        "2019/52/07\t2020/01/01\t2020/01/02\t2020/01/03\t2020/53/04\t2020/53/05"]

    # Parsing dates around Dec 31.
    result = self.client.execute(
        "select cast('2019/52/07' as date format 'IYYY/IW/ID'), "
        "       cast('2020/01/01' as date format 'IYYY/IW/ID'), "
        "       cast('2020/01/02' as date format 'IYYY/IW/ID'), "
        "       cast('2020/01/03' as date format 'IYYY/IW/ID'), "
        "       cast('2020/53/04' as date format 'IYYY/IW/ID'), "
        "       cast('2020/53/05' as date format 'IYYY/IW/ID')")
    assert result.data == [
        "2019-12-29\t2019-12-30\t2019-12-31\t2020-01-01\t2020-12-31\t2021-01-01"]

    err = self.execute_query_expect_failure(self.client,
        "select cast('2019/53/01' as date format 'IYYY/IW/ID')")
    assert (r'''String to Date parse failed. Input '2019/53/01' doesn't match with '''
        r'''format 'IYYY/IW/ID''' in str(err))

    # Format 4, 3, 2, 1-digit week numbering year.
    # 2020-01-01 is Wednesday, belongs to week 1 of year 2020.
    query_options = dict({'now_string': '2019-01-01 11:11:11'})
    result = self.execute_query(
        "select cast(date'2020-01-01' as string format 'IYYY/IW/ID'), "
        "       cast(date'2020-01-01' as string format 'IYY/IW/ID'), "
        "       cast(date'2020-01-01' as string format 'IY/IW/ID'), "
        "       cast(date'2020-01-01' as string format 'I/IW/ID')", query_options)
    assert result.data == ["2020/01/03\t020/01/03\t20/01/03\t0/01/03"]

    # Parse 4, 3, 2, 1-digit week numbering year.
    result = self.execute_query(
        "select cast('2020/01/03' as date format 'IYYY/IW/ID'), "
        "       cast('020/01/03' as date format 'IYYY/IW/ID'), "
        "       cast('20/01/03' as date format 'IYYY/IW/ID'), "
        "       cast('0/01/03' as date format 'IYYY/IW/ID'), "
        "       cast('020/01/03' as date format 'IYY/IW/ID'), "
        "       cast('20/01/03' as date format 'IYY/IW/ID'), "
        "       cast('0/01/03' as date format 'IYY/IW/ID'), "
        "       cast('20/01/03' as date format 'IY/IW/ID'), "
        "       cast('0/01/03' as date format 'IY/IW/ID'), "
        "       cast('0/01/03' as date format 'I/IW/ID')", query_options)
    assert result.data == ['2020-01-01\t2020-01-01\t2020-01-01\t2010-01-06\t'
                           '2020-01-01\t2020-01-01\t2010-01-06\t'
                           '2020-01-01\t2010-01-06\t'
                           '2010-01-06']

    # 2000-01-01 is Saturday, so it belongs to the 1999 ISO 8601 week-numbering year.
    # Test that 1999 is used for prefixing 3, 2, 1-digit week numbering year.
    query_options = dict({'now_string': '2000-01-01 11:11:11'})
    result = self.execute_query(
        "select cast('2005/01/01' as date format 'IYYY/IW/ID'), "
        "       cast('005/01/01' as date format 'IYYY/IW/ID'), "
        "       cast('05/01/01' as date format 'IYYY/IW/ID'), "
        "       cast('5/01/01' as date format 'IYYY/IW/ID'), "
        "       cast('05/01/01' as date format 'IY/IW/ID'), "
        "       cast('5/01/01' as date format 'IY/IW/ID'), "
        "       cast('5/01/01' as date format 'I/IW/ID')", query_options)
    assert result.data == ['2005-01-03\t1004-12-31\t1905-01-02\t1995-01-02\t'
                           '1905-01-02\t1995-01-02\t'
                           '1995-01-02']

    # Parse 1-digit week of year and 1-digit week day.
    result = self.client.execute(
        "select cast('2020/53/4' as date format 'IYYY/IW/ID'), "
        "       cast('2020/1/3' as date format 'IYYY/IW/ID')")
    assert result.data == ["2020-12-31\t2020-01-01"]

    # Parse dayname with week-based tokens
    result = self.client.execute(
        "select cast('2020/wed/1' as date format 'IYYY/DY/IW'), "
        "       cast('2020/wed1' as date format 'iyyy/dyiw'), "
        "       cast('2020wed1' as date format 'IYYYDYIW'), "
        "       cast('2020WEd1' as date format 'iyyydyiw'), "
        "       cast('2020/wednesday/1' as date format 'IYYY/DAY/IW'), "
        "       cast('2020/wednesday1' as date format 'iyyy/dayiw'), "
        "       cast('2020wednesday1' as date format 'IYYYDAYIW'), "
        "       cast('2020wEdnESday1' as date format 'iyyydayiw')")
    assert result.data == ["2020-01-01\t2020-01-01\t2020-01-01\t2020-01-01\t"
                           "2020-01-01\t2020-01-01\t2020-01-01\t2020-01-01"]

  def test_fm_fx_modifiers(self):
    # Exact mathcing for the whole format.
    result = self.client.execute("select cast('2001-03-01 03:10:15.123456 -01:30' as "
        "timestamp format 'FXYYYY-MM-DD HH12:MI:SS.FF6 TZH:TZM')")
    assert result.data == ["2001-03-01 03:10:15.123456000"]

    # Strict separator matching.
    result = self.client.execute("select cast('2001-03-02 03:10:15' as timestamp format"
        "'FXYYYY MM-DD HH12:MI:SS')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2001-03-03 03:10:15' as timestamp format"
        "'FXYYYY-MM-DD HH12::MI:SS')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2001-03-04    ' as timestamp format"
        "'FXYYYY-MM-DD ')")
    assert result.data == ["NULL"]

    # Strict matching of single quote separator.
    result = self.client.execute(r'''select cast('2001\'04-01' as timestamp format'''
        r''' 'FXYYYY\'MM-DD')''')
    assert result.data == ["2001-04-01 00:00:00"]

    result = self.client.execute(r'''select cast("2001'04-02" as date format'''
        r''' 'FXYYYY\'MM-DD')''')
    assert result.data == ["2001-04-02"]

    result = self.client.execute(r'''select cast('2001\'04-03' as timestamp format'''
        r''' "FXYYYY'MM-DD")''')
    assert result.data == ["2001-04-03 00:00:00"]

    result = self.client.execute(r'''select cast("2001'04-04" as date format'''
        r''' "FXYYYY'MM-DD")''')
    assert result.data == ["2001-04-04"]

    # Strict token length matching.
    result = self.client.execute("select cast('2001-3-05' as timestamp format "
        "'FXYYYY-MM-DD')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('15-03-06' as timestamp format "
        "'FXYYYY-MM-DD')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('15-03-07' as date format 'FXYY-MM-DD')")
    assert result.data == ["2015-03-07"]

    result = self.client.execute("select cast('2001-03-08 03:15:00 AM' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SS PM')")
    assert result.data == ["2001-03-08 03:15:00"]

    result = self.client.execute("select cast('2001-03-08 03:15:00 AM' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SS P.M.')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2001-03-09 03:15:00.1234' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SS.FF4')")
    assert result.data == ["2001-03-09 03:15:00.123400000"]

    result = self.client.execute("select cast('2001-03-09 03:15:00.12345' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SS.FF4')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2001-03-09 03:15:00.12345' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SS.FF')")
    assert result.data == ["NULL"]

    # Strict week-based token length matching.
    result = self.client.execute(
        "select cast('2015/3/05' as timestamp format 'FXIYYY/IW/ID'), "
        "       cast('2015/03/5' as timestamp format 'FXIYYY/IW/ID'), "
        "       cast('015/03/05' as timestamp format 'FXIYYY/IW/ID'), "
        "       cast('15/03/05' as timestamp format 'FXIYYY/IW/ID'), "
        "       cast('5/03/05' as timestamp format 'FXIYYY/IW/ID')")
    assert result.data == ["NULL\tNULL\tNULL\tNULL\tNULL"]
    err = self.execute_query_expect_failure(self.client,
        "select cast('2015/3/05' as date format 'FXIYYY/IW/ID')")
    assert (r'''String to Date parse failed. Input '2015/3/05' doesn't match with '''
        r'''format 'FXIYYY/IW/ID''' in str(err))

    query_options = dict({'now_string': '2019-01-01 11:11:11'})
    result = self.execute_query(
        "select cast('2015/03/05' as timestamp format 'FXIYYY/IW/ID'), "
        "       cast('015/03/05' as timestamp format 'FXIYY/IW/ID'), "
        "       cast('15/03/05' as timestamp format 'FXIY/IW/ID'), "
        "       cast('5/03/05' as timestamp format 'FXI/IW/ID'), "
        "       cast('2015/03/05' as date format 'FXIYYY/IW/ID'), "
        "       cast('015/03/05' as date format 'FXIYY/IW/ID'), "
        "       cast('15/03/05' as date format 'FXIY/IW/ID'), "
        "       cast('5/03/05' as date format 'FXI/IW/ID')", query_options)
    assert result.data == ["2015-01-16 00:00:00\t2015-01-16 00:00:00\t"
        "2015-01-16 00:00:00\t2015-01-16 00:00:00\t"
        "2015-01-16\t2015-01-16\t2015-01-16\t2015-01-16"]

    # Strict token length matching with text token containing escaped double quote.
    result = self.client.execute(r'''select cast('2001-03-09 some "text03:25:00' '''
        r'''as timestamp format "FXYYYY-MM-DD \"some \\\"text\"HH12:MI:SS")''')
    assert result.data == ["2001-03-09 03:25:00"]

    # Use FM to ignore FX modifier for some of the tokens.
    result = self.client.execute("select cast('2001-03-10 03:15:00.12345' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SS.FMFF')")
    assert result.data == ["2001-03-10 03:15:00.123450000"]

    result = self.client.execute("select cast('019-03-10 04:15:00' as timestamp "
        "format 'FXFMYYYY-MM-DD HH12:MI:SS')")
    assert result.data == ["2019-03-10 04:15:00"]

    result = self.client.execute("select cast('2004-03-08 03:15:00 AM' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SS FMP.M.')")
    assert result.data == ["2004-03-08 03:15:00"]

    # Multiple FM modifiers in a format.
    result = self.client.execute("select cast('2001-3-11 3:15:00.12345' as timestamp "
        "format 'FXYYYY-FMMM-DD FMHH12:MI:SS.FMFF')")
    assert result.data == ["2001-03-11 03:15:00.123450000"]

    result = self.client.execute("select cast('2001-3-11 3:15:30' as timestamp "
        "format 'FXYYYY-FMMM-DD FMFMHH12:MI:SS')")
    assert result.data == ["2001-03-11 03:15:30"]

    # FM modifier effects only the next token.
    result = self.client.execute("select cast('2001-3-12 3:1:00.12345' as timestamp "
        "format 'FXYYYY-FMMM-DD FMHH12:MI:SS.FMFF')")
    assert result.data == ["NULL"]

    # FM modifier before text token is valid for the text token and not for the token
    # right after the text token.
    result = self.client.execute(r'''select cast('1999-10text1' as timestamp format '''
        ''' 'FXYYYY-MMFM"text"DD')''')
    assert result.data == ["NULL"]

    # FM modifier skips the separators and affects the next non-separator token.
    result = self.client.execute(r'''select cast('1999-10-2' as timestamp format '''
        ''' 'FXYYYY-MMFM-DD')''')
    assert result.data == ["1999-10-02 00:00:00"]

    # FM modifier at the end has no effect.
    result = self.client.execute("select cast('2001-03-13 03:01:00' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SSFM')")
    assert result.data == ["2001-03-13 03:01:00"]

    result = self.client.execute("select cast('2001-03-13 03:01:0' as timestamp "
        "format 'FXYYYY-MM-DD HH12:MI:SSFM')")
    assert result.data == ["NULL"]

    # In a datetime to string path FX is the default so it works with FX as it would
    # without.
    result = self.client.execute("select cast(cast('2001-03-05 03:10:15.123456' as "
        "timestamp) as string format 'FXYYYY-MM-DD HH24:MI:SS.FF7')")
    assert result.data == ["2001-03-05 03:10:15.1234560"]

    result = self.client.execute(
        "select cast(date'0001-01-10' as string format 'FXIYYY-IW-ID'), "
        "       cast(date'0001-10-10' as string format 'FXIYYY-IW-ID')")
    assert result.data == ["0001-02-03\t0001-41-03"]

    # Datetime to string path: Tokens with FM modifier don't pad output to a given
    # length.
    result = self.client.execute("select cast(cast('2001-03-14 03:06:08' as timestamp) "
        "as string format 'YYYY-MM-DD FMHH24:FMMI:FMSS')")
    assert result.data == ["2001-03-14 3:6:8"]

    result = self.client.execute("select cast(cast('0001-03-09' as date) "
        "as string format 'FMYYYY-FMMM-FMDD')")
    assert result.data == ["1-3-9"]

    result = self.client.execute("select cast(date'0001-03-10' as string format "
        "'FMYY-FMMM-FMDD')")
    assert result.data == ["1-3-10"]

    result = self.client.execute(
        "select cast(date'0001-01-10' as string format 'FMIYYY-FMIW-FMID'), "
        "       cast(date'0001-10-10' as string format 'FMIYYY-FMIW-FMID')")
    assert result.data == ["1-2-3\t1-41-3"]

    # Datetime to string path: FM modifier is effective even if FX modifier is also
    # given.
    result = self.client.execute("select cast(cast('2001-03-15 03:06:08' as "
        "timestamp) as string format 'FXYYYY-MM-DD FMHH24:FMMI:FMSS')")
    assert result.data == ["2001-03-15 3:6:8"]

    result = self.client.execute("select cast(cast('0001-04-09' as date) "
        "as string format 'FXYYYY-FMMM-FMDD')")
    assert result.data == ["0001-4-9"]

    result = self.client.execute("select cast(cast('0001-04-10' as date) "
        "as string format 'FXFMYYYY-FMMM-FMDD')")
    assert result.data == ["1-4-10"]

    result = self.client.execute(
        "select cast(date'0001-01-10' as string format 'FXFMIYYY-FMIW-FMID'), "
        "       cast(date'0001-10-10' as string format 'FXFMIYYY-FMIW-FMID')")
    assert result.data == ["1-2-3\t1-41-3"]

    # FX and FM modifiers are case-insensitive.
    result = self.client.execute("select cast('2019-5-10' as date format "
        "'fxYYYY-fmMM-DD')")
    assert result.data == ["2019-05-10"]


  def test_quarter(self):
    result = self.client.execute("select cast(date'2001-01-01' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 1 01"]

    result = self.client.execute("select cast(date'2001-03-31' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 1 03"]

    result = self.client.execute("select cast(date'2001-4-1' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 2 04"]

    result = self.client.execute("select cast(date'2001-6-30' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 2 06"]

    result = self.client.execute("select cast(date'2001-7-1' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 3 07"]

    result = self.client.execute("select cast(date'2001-9-30' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 3 09"]

    result = self.client.execute("select cast(date'2001-10-1' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 4 10"]

    result = self.client.execute("select cast(date'2001-12-31' as string "
        "FORMAT 'YYYY Q MM')")
    assert result.data == ["2001 4 12"]

  def test_format_parse_errors(self):
    # Invalid format
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'XXXX-dd-MM')")
    assert "Bad date/time conversion format: XXXX-dd-MM" in str(err)

    # Invalid use of SimpleDateFormat
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01 15:10' as timestamp format 'yyyy-MM-dd +hh:mm')")
    assert "Bad date/time conversion format: yyyy-MM-dd +hh:mm" in str(err)

    # Duplicate format element
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD MM')")
    assert "Invalid duplication of format element" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-YYYY')")
    assert "Invalid duplication of format element" in str(err)

    # Multiple year tokens provided
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-YY')")
    assert "Multiple year tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYY-MM-DD-Y')")
    assert "Multiple year tokens provided" in str(err)

    # Year and round year conflict
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YY-MM-DD-RRRR')")
    assert "Both year and round year are provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'RR-MM-DD-YYY')")
    assert "Both year and round year are provided" in str(err)

    # Quarter token not allowed in a string to datetime conversion.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-1-01' as timestamp format 'YYYY-Q-DDD')")
    assert "Quarter token is not allowed in a string to datetime conversion" in str(err)

    # Conflict between MM, MONTH and MON tokens
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-MONTH')")
    assert "Multiple month tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-MON')")
    assert "Multiple month tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MONTH-DD-MON')")
    assert "Multiple month tokens provided" in str(err)

    # Conflict between DAY, DY and ID tokens.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01-Monday' as timestamp format 'IYYY-IW-ID-DAY')")
    assert "Multiple day of week tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01-Mon' as timestamp format 'IYYY-IW-ID-DY')")
    assert "Multiple day of week tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-Monday-Mon' as timestamp format 'IYYY-IW-DAY-DY')")
    assert "Multiple day of week tokens provided" in str(err)

    # Week of year token not allowed in a string to datetime conversion.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-1-01' as timestamp format 'YYYY-WW-DD')")
    assert "Week number token is not allowed in a string to datetime conversion" in \
        str(err)

    # Week of month token not allowed in a string to datetime conversion.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-1-01' as timestamp format 'YYYY-W-DD')")
    assert "Week number token is not allowed in a string to datetime conversion" in \
        str(err)

    # Day of year conflict
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DDD')")
    assert "Day of year provided with day or month token" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-DD-DDD')")
    assert "Day of year provided with day or month token" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-MAY-01' as timestamp format 'YYYY-MONTH-DDD')")
    assert "Day of year provided with day or month token" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-JUN-01' as timestamp format 'YYYY-MON-DDD')")
    assert "Day of year provided with day or month token" in str(err)

    # Day of week token not allowed in a string to datetime conversion.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-1-02' as timestamp format 'YYYY-D-MM')")
    assert "Day of week token is not allowed in a string to datetime conversion" in \
        str(err)

    # Day name token not allowed in a string to datetime conversion.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-1-02 Monday' as timestamp format 'YYYY-DD-MM DAY')")
    assert "Day name token is not allowed in a string to datetime conversion except " \
        "with IYYY|IYY|IY|I and IW tokens" in str(err)

    # Conflict between hour tokens
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH:HH24')")
    assert "Multiple hour tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH12:HH24')")
    assert "Multiple hour tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH12:HH')")
    assert "Multiple hour tokens provided" in str(err)

    # Conflict with median indicator
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD AM HH:MI A.M.')")
    assert "Multiple median indicator tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD PM HH:MI am')")
    assert "Multiple median indicator tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH24:MI a.m.')")
    assert "Conflict between median indicator and hour token" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD p.m.')")
    assert "Missing hour token" in str(err)

    # Conflict with second of day
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD SSSSS HH')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH12:SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH24SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD MI SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD SS SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    # Too long format
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format '" +
        "{char: <101}".format(char="s") + "')")
    assert "The input format is too long" in str(err)

    # Timezone offsets in a datetime to string formatting
    err = self.execute_query_expect_failure(self.client,
        "select cast(cast('2017-05-01 01:15' as timestamp format 'YYYY-MM-DD TZH:TZM') "
        "as string format 'TZH')")
    assert "Timezone offset not allowed in a datetime to string conversion" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast(cast('2017-05-01 01:15' as timestamp format 'YYYY-MM-DD TZH:TZM') "
        "as string format 'TZM')")
    assert "Timezone offset not allowed in a datetime to string conversion" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast(cast('2017-05-01 01:15' as timestamp format 'YYYY-MM-DD TZH:TZM') "
        "as string format 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert "Timezone offset not allowed in a datetime to string conversion" in str(err)

    # TZM requires TZH
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-12-31 08:00 AM 59' as timestamp FORMAT "
        "'YYYY-MM-DD HH12:MI A.M. TZM')")
    assert "TZH token is required for TZM" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-12-31 08:00 AM -59' as timestamp FORMAT "
        "'YYYY-MM-DD HH12:MI A.M. TZM')")
    assert "TZH token is required for TZM" in str(err)

    # Multiple fraction second token conflict
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'YYYY-MM-DD FF FF1')")
    assert "Multiple fractional second tokens provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'YYYY-MM-DD FF2 FF3')")
    assert "Multiple fractional second tokens provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'YYYY-MM-DD FF4 FF5')")
    assert "Multiple fractional second tokens provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'YYYY-MM-DD FF6 FF7')")
    assert "Multiple fractional second tokens provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'YYYY-MM-DD FF8 FF9')")
    assert "Multiple fractional second tokens provided." in str(err)

    # No date token
    err = self.execute_query_expect_failure(self.client,
        "select cast('2020-05-05' as timestamp format 'FF1')")
    assert "No date tokens provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2020-05-05' as timestamp format 'SSSSS')")
    assert "No date tokens provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2020-05-05' as timestamp format 'HH:MI:SS')")
    assert "No date tokens provided." in str(err)

    # ISO 8601 Week-based and normal date pattern tokens must not be mixed.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-01' as date format 'IYYY-MM-ID')")
    assert "ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) are not allowed to be " \
           "used with regular date tokens." in str(err)
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-01 01:00' as timestamp format 'IYYY-MM-ID HH24:MI')")
    assert "ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) are not allowed to be " \
           "used with regular date tokens." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-01' as date format 'YYYY-IW-DD')")
    assert "ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) are not allowed to be " \
           "used with regular date tokens." in str(err)
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-01' as timestamp format 'IYYY-IW-DD')")
    assert "ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) are not allowed to be " \
           "used with regular date tokens." in str(err)

    # Missing ISO 8601 week-based pattern tokens.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10' as date format 'IYYY-IW')")
    assert "One or more required ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) " \
           "are missing." in str(err)
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10 01:00' as timestamp format 'IYYY-IW HH24:MI')")
    assert "One or more required ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) " \
           "are missing." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('18-07' as date format 'IY-ID')")
    assert "One or more required ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) " \
           "are missing." in str(err)
    err = self.execute_query_expect_failure(self.client,
        "select cast('18-07 01:00' as timestamp format 'IY-ID HH24:MI')")
    assert "One or more required ISO 8601 week-based date tokens (i.e. IYYY, IW, ID) " \
           "are missing." in str(err)

    # ISO 8601 Week numbering year conflict
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-018-10-01' as date format 'IYYY-IYY-IW-DD')")
    assert "Multiple year tokens provided" in str(err)
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-018-10-01 01:00' as timestamp format "
        "'IYYY-IYY-IW-DD HH24:MI')")
    assert "Multiple year tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('018-8-10-01' as date format 'IYY-I-IW-DD')")
    assert "Multiple year tokens provided" in str(err)
    err = self.execute_query_expect_failure(self.client,
        "select cast('018-8-10-01 01:00' as timestamp format 'IYY-I-IW-DD HH24:MI')")
    assert "Multiple year tokens provided" in str(err)

    # Verify that conflict check is not skipped when format ends with separators.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-RR--')")
    assert "Both year and round year are provided" in str(err)

    # Unclosed quotation in text pattern
    err = self.execute_query_expect_failure(self.client,
        r'''select cast('1985-11-20text' as timestamp format 'YYYY-MM-DD"text')''')
    assert "Missing closing quotation mark." in str(err)

    err = self.execute_query_expect_failure(self.client,
        r'''select cast('1985-11-21text' as timestamp format 'YYYY-MM-DD\"text"')''')
    assert "Missing closing quotation mark." in str(err)

    err = self.execute_query_expect_failure(self.client,
        r'''select cast(date"1985-12-08" as string format 'YYYY-MM-DD \"X"');''')
    assert "Missing closing quotation mark." in str(err)

    err = self.execute_query_expect_failure(self.client,
        r'''select cast(date"1985-12-09" as string format 'YYYY-MM-DD "X');''')
    assert "Missing closing quotation mark." in str(err)

    # Format containing text token only.
    err = self.execute_query_expect_failure(self.client,
        r'''select cast("1985-11-29" as date format '" some text "')''')
    assert "No datetime tokens provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        r'''select cast(cast("1985-12-02" as date) as string format "\"free text\"")''')
    assert "No datetime tokens provided." in str(err)

    # FX modifier not at the begining of the format.
    err = self.execute_query_expect_failure(self.client,
        'select cast("2001-03-01 00:10:02" as timestamp format '
        '"YYYY-MM-DD FXHH12:MI:SS")')
    assert "FX modifier should be at the beginning of the format string." in str(err)

    err = self.execute_query_expect_failure(self.client,
        'select cast("2001-03-01 00:10:02" as timestamp format '
        '"YYYY-MM-DD HH12:MI:SS FX")')
    assert "FX modifier should be at the beginning of the format string." in str(err)

    err = self.execute_query_expect_failure(self.client,
        'select cast(date"2001-03-01" as string format "YYYYFX-MM-DD")')
    assert "FX modifier should be at the beginning of the format string." in str(err)

    err = self.execute_query_expect_failure(self.client,
        'select cast(date"2001-03-02" as string format "FXFMFXYYYY-MM-DD")')
    assert "FX modifier should be at the beginning of the format string." in str(err)

    err = self.execute_query_expect_failure(self.client,
        'select cast(date"2001-03-03" as string format "FXFXYYYY-MM-DD")')
    assert "FX modifier should be at the beginning of the format string." in str(err)

    err = self.execute_query_expect_failure(self.client,
        'select cast(date"2001-03-04" as string format "FMFXYYYY-MM-DD")')
    assert "FX modifier should be at the beginning of the format string." in str(err)

    err = self.execute_query_expect_failure(self.client,
        'select cast(date"2001-03-03" as string format "-FXYYYY-MM-DD")')
    assert "FX modifier should be at the beginning of the format string." in str(err)

    err = self.execute_query_expect_failure(self.client,
        r'''select cast(date"2001-03-03" as string format '"text"FXYYYY-MM-DD')''')
    assert "FX modifier should be at the beginning of the format string." in str(err)

  def test_varchar_cast(self, unique_database):
    table = "{0}.test_varchar_casts".format(unique_database)
    self.execute_query("create table {0} (c char(6), v varchar(6))".format(table))
    self.execute_query("insert into {0} values (cast('test' as char(6)), "
        "cast('test' as varchar(6))), (cast('tester' as char(6)), "
        "cast('tester' as varchar(6)))".format(table))

    # Compare char to varchar
    select_star = "select * from " + table
    assert ['test  \ttest', 'tester\ttester'] == self.execute_query(
        select_star + " where c = cast(v as char(6))").data
    assert ['tester\ttester'] == self.execute_query(
        select_star + " where v = cast(c as varchar(6))").data
    assert ['tester\ttester'] == self.execute_query(
        select_star + " where v = cast(c as varchar)").data
    # Newly supported cases in IMPALA-10086
    assert ['tester\ttester'] == self.execute_query(
        select_star + " where c = v").data
    assert ['tester\ttester'] == self.execute_query(
        select_star + " where v = c").data

    # Compare char to literal
    select_c = "select c from " + table
    assert [] == self.execute_query(select_c + " where c = 'test'").data
    assert ['test  '] == self.execute_query(
        select_c + " where c = 'test  '").data
    assert ['tester'] == self.execute_query(
        select_c + " where c = 'tester'").data
    assert ['test  '] == self.execute_query(
        select_c + " where c = cast('test' as char(6))").data
    assert ['tester'] == self.execute_query(
        select_c + " where c = cast('tester' as char(6))").data
    # Newly supported cases in IMPALA-10086
    assert [] == self.execute_query(
        select_c + " where c = cast('test' as varchar(6))").data
    assert ['test  '] == self.execute_query(
        select_c + " where c = cast('test  ' as varchar(6))").data
    assert ['tester'] == self.execute_query(
        select_c + " where c = cast('tester' as varchar(6))").data

    # Compare varchar to literal
    select_v = "select v from " + table
    assert ['test'] == self.execute_query(
        select_v + " where v = 'test'").data
    assert ['tester'] == self.execute_query(
        select_v + " where v = 'tester'").data
    assert ['test'] == self.execute_query(
        select_v + " where v = cast('test' as varchar(6))").data
    assert ['tester'] == self.execute_query(
        select_v + " where v = cast('tester' as varchar(6))").data
    # Newly supported cases in IMPALA-10086
    assert [] == self.execute_query(
        select_v + " where v = cast('test' as char(6))").data
    assert ['test'] == self.execute_query(
        select_v + " where v = cast('test' as char(4))").data
    assert ['tester'] == self.execute_query(
        select_v + " where v = cast('tester' as char(6))").data
