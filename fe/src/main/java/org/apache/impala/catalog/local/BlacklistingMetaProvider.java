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

package org.apache.impala.catalog.local;

import static org.apache.impala.util.CatalogBlacklistUtils.isDbBlacklisted;
import static org.apache.impala.util.CatalogBlacklistUtils.isTableBlacklisted;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

/**
 * A MetaProvider that wraps another MetaProvider and filters out blacklisted
 * databases and tables based on the blacklists defined in
 * CatalogBlacklistUtils.
 *
 * This class implements the adapter design pattern.
 */
public class BlacklistingMetaProvider implements MetaProvider {
  private final MetaProvider delegate;

  public BlacklistingMetaProvider(MetaProvider delegate) {
    this.delegate = delegate;
  }

  public String getURI() {
    return this.delegate.getURI();
  }
  public AuthorizationPolicy getAuthPolicy() {
    return this.delegate.getAuthPolicy();
  }

  public boolean isReady() {
    return this.delegate.isReady();
  }

  public void waitForIsReady(long timeoutMs) {
    this.delegate.waitForIsReady(timeoutMs);
  }

  public void setIsReady(boolean isReady) {
    this.delegate.setIsReady(isReady);
  }

  public ImmutableList<String> loadDbList() throws TException {
    return this.delegate.loadDbList().parallelStream().filter(
        dbName -> !isDbBlacklisted(dbName)).collect(ImmutableList.toImmutableList());
  }

  public Database loadDb(String dbName) throws TException {
    return this.delegate.loadDb(dbName);
  }

  public ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws MetaException, UnknownDBException, TException {
    return this.delegate.loadTableList(dbName).parallelStream().filter(
        tableMeta -> !isTableBlacklisted(dbName, tableMeta.getName()))
            .collect(ImmutableList.toImmutableList());
  }

  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException {
    return this.delegate.loadTable(dbName, tableName);
  }

  public Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tableName) {
    return this.delegate.getTableIfPresent(dbName, tableName);
  }

  public String loadNullPartitionKeyValue()
      throws MetaException, TException {
    return this.delegate.loadNullPartitionKeyValue();
  }

  public List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws MetaException, TException {
    return this.delegate.loadPartitionList(table);
  }

  public SqlConstraints loadConstraints(TableMetaRef table,
      Table msTbl) throws MetaException, TException {
    return this.delegate.loadConstraints(table, msTbl);
  }

  public List<String> loadFunctionNames(String dbName) throws TException {
    return this.delegate.loadFunctionNames(dbName);
  }

  public ImmutableList<Function> loadFunction(String dbName, String functionName)
      throws TException {
    return this.delegate.loadFunction(dbName, functionName);
  }

  public ImmutableList<DataSource> loadDataSources() throws TException {
    return this.delegate.loadDataSources();
  }

  public DataSource loadDataSource(String dsName) throws TException {
    return this.delegate.loadDataSource(dsName);
  }

  public Map<String, PartitionMetadata> loadPartitionsByRefs(TableMetaRef table,
      List<String> partitionColumnNames, ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs)
      throws MetaException, TException, CatalogException {
    return this.delegate.loadPartitionsByRefs(table, partitionColumnNames, hostIndex,
        partitionRefs);
  }

  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    return this.delegate.loadTableColumnStatistics(table, colNames);
  }

  public TPartialTableInfo loadIcebergTable(
      final TableMetaRef table) throws TException {
    return this.delegate.loadIcebergTable(table);
  }

  public org.apache.iceberg.Table loadIcebergApiTable(
      final TableMetaRef table, TableParams param, Table msTable) throws TException {
    return this.delegate.loadIcebergApiTable(table, param, msTable);
  }

  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref) {
    return this.delegate.getValidWriteIdList(ref);
  }

  public Iterable<HdfsCachePool> getHdfsCachePools() {
    return this.delegate.getHdfsCachePools();
  }

}
