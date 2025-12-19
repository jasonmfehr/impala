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

import java.util.Set;

import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Db;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public final class CatalogBlacklistUtils {
  private static Set<String> BLACKLISTED_DBS =
      CatalogBlacklistUtils.parseBlacklistedDbsFromConfigs();
  private static Set<TableName> BLACKLISTED_TABLES =
      CatalogBlacklistUtils.parseBlacklistedTablesFromConfigs();

  /**
   * Class contains only static functions.
   */
  private CatalogBlacklistUtils() {
  }


  /**
   * Helper method for tests to re-build the dbs and tables blacklists.
   * This method re-parses the blacklist configurations from {@link BackendConfig}
   * and updates the static BLACKLISTED_DBS and BLACKLISTED_TABLES {@link Set}s.
   * Should be called after backend configuration changes in tests.
   */
  public static void reload() {
    BLACKLISTED_DBS = CatalogBlacklistUtils.parseBlacklistedDbsFromConfigs();
    BLACKLISTED_TABLES = CatalogBlacklistUtils.parseBlacklistedTablesFromConfigs();
  }

  /**
   * Parse blacklisted databases from backend configs.
   * Retrieves the blacklisted databases configuration string from {@link BackendConfig}
   * and delegates to {@link #parseBlacklistedDbs(String, Logger)} for parsing.
   *
   * @return a set of lowercase database names that are blacklisted, or an empty
   *         set if no databases are blacklisted or {@link BackendConfig} is not
   *         initialized.
   */
  private static Set<String> parseBlacklistedDbsFromConfigs() {
    return parseBlacklistedDbs(
        BackendConfig.INSTANCE == null ? "" : BackendConfig.INSTANCE.getBlacklistedDbs(),
        null);
  }

  /**
   * Parse blacklisted tables from backend configs.
   * Retrieves the blacklisted tables configuration string from {@link BackendConfig}
   * and delegates to {@link #parseBlacklistedTables(String, Logger)} for parsing.
   *
   * @return a set of {@link TableName} objects representing blacklisted tables, or an
   *         empty set if no tables are blacklisted or {@link BackendConfig} is not
   *         initialized.
   */
  private static Set<TableName> parseBlacklistedTablesFromConfigs() {
    return parseBlacklistedTables(
        BackendConfig.INSTANCE == null ? "" :
            BackendConfig.INSTANCE.getBlacklistedTables(),
        null);
  }

  /**
   * Parse blacklisted databases from given configs string.
   * The input string should be a comma-separated list of database names. Database names
   * are converted to lowercase. Empty strings and whitespace are ignored.
   *
   * @param blacklistedDbsConfig a comma-separated string of database names to blacklist,
   *                             must not be null
   * @param logger optional {@link Logger} for logging parsed database names, can be null
   *               if logging is not required
   * @return a set of lowercase database names that are blacklisted
   */
  private static Set<String> parseBlacklistedDbs(String blacklistedDbsConfig,
      Logger logger) {
    Preconditions.checkNotNull(blacklistedDbsConfig);
    Set<String> blacklistedDbs = Sets.newHashSet();
    for (String db: Splitter.on(',').trimResults().omitEmptyStrings().split(
        blacklistedDbsConfig)) {
      blacklistedDbs.add(db.toLowerCase());
      if (logger != null) logger.info("Blacklist db: " + db);
    }
    return blacklistedDbs;
  }

  /**
   * Parse blacklisted tables from configs string.
   * The input string should be a comma-separated list of table names in the format
   * "database.table". Invalid table names are logged as warnings and skipped.
   * Empty strings and whitespace are ignored.
   *
   * @param blacklistedTablesConfig a comma-separated string of table names to blacklist,
   *                                must not be null
   * @param logger optional {@link Logger} for logging parsed table names and warnings
   *               about invalid table names, can be null if logging is not required
   * @return a set of {@link TableName} objects representing blacklisted tables
   */
  private static Set<TableName> parseBlacklistedTables(String blacklistedTablesConfig,
      Logger logger) {
    Preconditions.checkNotNull(blacklistedTablesConfig);
    Set<TableName> blacklistedTables = Sets.newHashSet();
    for (String tblName: Splitter.on(',').trimResults().omitEmptyStrings().split(
        blacklistedTablesConfig)) {
      TableName tbl = TableName.parse(tblName);
      if (tbl == null) {
        if (logger != null) {
          logger.warn(String.format("Illegal blacklisted table name: '%s'",
              tblName));
        }
        continue;
      }
      blacklistedTables.add(tbl);
      if (logger != null) logger.info("Blacklist table: " + tbl);
    }
    return blacklistedTables;
  }

  /**
   * Verify that a database name is not blacklisted.
   *
   * @param dbName the name of the database to verify
   * @throws AnalysisException if the database name is blacklisted
   */
  public static void verifyDbName(String dbName) throws AnalysisException {
    if (isDbBlacklisted(dbName)) {
      throw new AnalysisException("Invalid db name: " + dbName
          + ". It has been blacklisted using --blacklisted_dbs");
    }
  }

  /**
   * Check if a database is blacklisted.
   * Note: The system database ({@link Db#SYS}) is exempt from blacklisting when
   * workload management is enabled in {@link BackendConfig}.
   *
   * @param dbName the name of the database to check
   * @return true if the database is blacklisted, false otherwise
   */
  public static boolean isDbBlacklisted(String dbName) {
    if (BackendConfig.INSTANCE.enableWorkloadMgmt() && dbName.equalsIgnoreCase(Db.SYS)) {
      // Override system DB for Impala system tables.
      return false;
    }
    return BLACKLISTED_DBS.contains(dbName);
  }

  /**
   * Verify that a table name is not blacklisted.
   *
   * @param table the {@link TableName} object representing the table to verify
   * @throws AnalysisException if the table name is blacklisted
   */
  public static void verifyTableName(TableName table) throws AnalysisException {
    if (isTableBlacklisted(table)) {
      throw new AnalysisException("Invalid table/view name: " + table
          + ". It has been blacklisted using --blacklisted_tables");
    }
  }

  /**
   * Check if a table is blacklisted.
   *
   * @param table the {@link TableName} object representing the table to check
   * @return true if the table is blacklisted, false otherwise
   */
  public static boolean isTableBlacklisted(TableName table) {
    return BLACKLISTED_TABLES.contains(table);
  }

  /**
   * Check if a table is blacklisted given its database and table name.
   * This is a convenience method that constructs a {@link TableName} object and delegates
   * to {@link #isTableBlacklisted(TableName)}.
   *
   * @param db the name of the database containing the table
   * @param table the name of the table to check
   * @return true if the table is blacklisted, false otherwise
   */
  public static boolean isTableBlacklisted(String db, String table) {
    return isTableBlacklisted(new TableName(db, table));
  }
}
