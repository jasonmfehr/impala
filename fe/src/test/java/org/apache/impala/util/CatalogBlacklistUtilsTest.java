// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.junit.Test;

public class CatalogBlacklistUtilsTest {

  @Test
  public void testParsingBlacklistedDbsHappyPath() throws AnalysisException {
    setBlacklist("db1,db2", "");
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db1"));
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db2"));
    assertFalse(CatalogBlacklistUtils.isDbBlacklisted("db3"));

    CatalogBlacklistUtils.verifyDbName("db3");
    try {
      CatalogBlacklistUtils.verifyDbName("db1");
      fail("Expected AnalysisException for blacklisted db");
    } catch (AnalysisException e) {
      assertThat(e.getMessage(), equalTo("Invalid db name: db1. It has been blacklisted "
          + "using --blacklisted_dbs"));
    }
  }

  @Test
  public void testParsingBlacklistedDbsNamesWithSpaces() {
    setBlacklist(" db1 , db2 ", "");
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db1"));
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db2"));
    assertFalse(CatalogBlacklistUtils.isDbBlacklisted("db3"));
  }

  @Test
  public void testParsingBlacklistedDbsCaseInsensitiveNames() {
    setBlacklist("DB1,Db2", "");
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db1"));
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db2"));
    assertFalse(CatalogBlacklistUtils.isDbBlacklisted("db3"));
  }

  @Test
  public void testParsingBlacklistedDbsInvalidNames() {
    setBlacklist("db1,", "");
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db1"));
    assertFalse(CatalogBlacklistUtils.isDbBlacklisted("db2"));
    assertFalse(CatalogBlacklistUtils.isDbBlacklisted("db3"));
  }

  @Test
  public void testParsingBlacklistedTablesHappyPath() throws AnalysisException {
    TableName foo = new TableName("db3", "foo");
    TableName baz = new TableName("db3", "baz");
    setBlacklist("", "db3.foo,db3.bar");

    assertTrue(CatalogBlacklistUtils.isTableBlacklisted(foo.getDb(), foo.getTbl()));
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted(foo));
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted("db3", "bar"));
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted(new TableName("db3", "bar")));
    assertFalse(CatalogBlacklistUtils.isTableBlacklisted(baz.getDb(), baz.getTbl()));
    assertFalse(CatalogBlacklistUtils.isTableBlacklisted(baz));

    CatalogBlacklistUtils.verifyTableName(baz);
    try {
      CatalogBlacklistUtils.verifyTableName(foo);
      fail("Expected AnalysisException for blacklisted table");
    } catch (AnalysisException e) {
      assertThat(e.getMessage(), equalTo("Invalid table/view name: " + foo
          + ". It has been blacklisted using --blacklisted_tables"));
    }
  }

  @Test
  public void testParsingBlacklistedTablesNamesWithInputSpaces() {
    setBlacklist("", " db3 . foo , db3 . bar  ");
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted("db3", "foo"));
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted("db3", "bar"));
    assertFalse(CatalogBlacklistUtils.isTableBlacklisted("db3", "baz"));
  }

  @Test
  public void testParsingBlacklistedTablesNamesWithoutDb() {
    setBlacklist("", "foo");
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted(Catalog.DEFAULT_DB, "foo"));
  }

  @Test
  public void testParsingBlacklistedTablesCaseInsensitiveNames() {
    setBlacklist("", "DB3.Foo,db3.Bar");
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted("db3", "foo"));
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted("db3", "bar"));
  }

  @Test
  public void testParsingBlacklistedTablesInvalidNames() {
    // Test abnormal inputs
    setBlacklist("", "db3.,.bar,,");
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted(Catalog.DEFAULT_DB, "bar"));
  }

  @Test
  public void testParsingBlacklistedDbsAndTables() {
    setBlacklist("db1,db2", "db3.foo,db3.bar");
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db1"));
    assertTrue(CatalogBlacklistUtils.isDbBlacklisted("db2"));
    assertFalse(CatalogBlacklistUtils.isDbBlacklisted("db3"));


    assertFalse(CatalogBlacklistUtils.isTableBlacklisted("db1", "foo"));
    assertFalse(CatalogBlacklistUtils.isTableBlacklisted("db2", "bar"));
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted("db3", "foo"));
    assertTrue(CatalogBlacklistUtils.isTableBlacklisted("db3", "bar"));
    assertFalse(CatalogBlacklistUtils.isTableBlacklisted("db3", "baz"));
  }

  public static void setBlacklist(String blacklistedDbs, String blacklistedTables) {
    TBackendGflags backendGflags = new TBackendGflags();
    backendGflags.setBlacklisted_dbs(blacklistedDbs);
    backendGflags.setBlacklisted_tables(blacklistedTables);
    BackendConfig.create(backendGflags, false);
    CatalogBlacklistUtils.reload();
  }

}
