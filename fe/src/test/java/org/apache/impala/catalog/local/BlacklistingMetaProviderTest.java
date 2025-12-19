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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.CatalogBlacklistUtilsTest;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class BlacklistingMetaProviderTest {

  @Test
  public void testLoadDbList() throws TException {
    // Configure backend with blacklisted databases.
    CatalogBlacklistUtilsTest.setBlacklist("blacklisted_db1,blacklisted_db2", "");

    // Create mock provider delegate that returns a list including both blacklisted and
    // non-blacklisted databases.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadDbList()).thenReturn(ImmutableList.of("allowed_db1",
        "blacklisted_db1", "allowed_db2", "blacklisted_db2", "allowed_db3"));

    // Create the blacklisting provider
    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);

    // Call loadDbList and verify blacklisted databases are filtered out.
    ImmutableList<String> result = fixture.loadDbList();

    // Should have 3 databases: allowed_db1, allowed_db2, allowed_db3.
    assertThat(result.size(), equalTo(3));
    assertTrue(result.contains("allowed_db1"));
    assertTrue(result.contains("allowed_db2"));
    assertTrue(result.contains("allowed_db3"));

    Mockito.verify(mockDelegate).loadDbList();
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testLoadDbListWithNonBlacklistedDbs() throws TException {
    // Configure backend with empty blacklist.
    CatalogBlacklistUtilsTest.setBlacklist("", "");

    // Create mock provider delegate that returns a list of databases.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadDbList()).thenReturn(ImmutableList.of("regular_db1",
        "regular_db2", "regular_db3"));

    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);
    ImmutableList<String> result = fixture.loadDbList();

    // Verify that all databases are returned.
    assertThat(result.size(), equalTo(3));
    assertTrue(result.contains("regular_db1"));
    assertTrue(result.contains("regular_db2"));
    assertTrue(result.contains("regular_db3"));

    Mockito.verify(mockDelegate).loadDbList();
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

    @SuppressWarnings("unchecked")
    @Test
  public void testLoadTableList() throws TException {
    // Configure backend with blacklisted tables.
    CatalogBlacklistUtilsTest.setBlacklist("", "db1.foo,db2.bar");

    // Create mock provider delegate that returns a list including both blacklisted and
    // non-blacklisted tables.
    MetaProvider mockDelegate = Mockito.mock(MetaProvider.class);
    Mockito.when(mockDelegate.loadTableList("db1")).thenReturn(ImmutableList.of(
        new TBriefTableMeta("foo"), new TBriefTableMeta("bar")));
    Mockito.when(mockDelegate.loadTableList("db2")).thenReturn(ImmutableList.of(
        new TBriefTableMeta("foo"), new TBriefTableMeta("bar")));
    Mockito.when(mockDelegate.loadTableList("db3")).thenReturn(ImmutableList.of(
        new TBriefTableMeta("foo"), new TBriefTableMeta("bar")));

    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockDelegate);

    assertThat(fixture.loadTableList("db1").toArray(),
        arrayContainingInAnyOrder(equalTo(new TBriefTableMeta("bar"))));

    assertThat(fixture.loadTableList("db2").toArray(),
        arrayContainingInAnyOrder(equalTo(new TBriefTableMeta("foo"))));

    assertThat(fixture.loadTableList("db3").toArray(), arrayContainingInAnyOrder(
        equalTo(new TBriefTableMeta("foo")), equalTo(new TBriefTableMeta("bar"))));

    Mockito.verify(mockDelegate).loadTableList("db1");
    Mockito.verify(mockDelegate).loadTableList("db2");
    Mockito.verify(mockDelegate).loadTableList("db3");
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testPassthroughFunctions() throws TException, MetaException,
      CatalogException {
    MetaProvider mockProvider = Mockito.mock(MetaProvider.class);
    BlacklistingMetaProvider fixture = new BlacklistingMetaProvider(mockProvider);

    final AuthorizationPolicy authPolicy = new AuthorizationPolicy();
    final Database database = new Database();
    final Table msTable = new Table();
    final MetaProvider.TableMetaRef tableMetaRef =
        Mockito.mock(MetaProvider.TableMetaRef.class);
    final Pair<Table, MetaProvider.TableMetaRef> tablePair =
        new Pair<>(msTable, tableMetaRef);
    final List<MetaProvider.PartitionRef> partitionRefs = new ArrayList<>();
    final SqlConstraints sqlConstraints =
        new SqlConstraints(new ArrayList<>(), new ArrayList<>());
    final ImmutableList<Function> functions = ImmutableList.of();
    final ImmutableList<DataSource> dataSources = ImmutableList.of();
    final DataSource dataSource =
        new DataSource("testDs", "/test/location", "TestClass", "V1");
    final Map<String, MetaProvider.PartitionMetadata> partitionMetadataMap =
        new HashMap<>();
    final List<ColumnStatisticsObj> columnStats = new ArrayList<>();
    final TPartialTableInfo partialTableInfo = new TPartialTableInfo();
    final org.apache.iceberg.Table icebergTable =
        Mockito.mock(org.apache.iceberg.Table.class);
    final TValidWriteIdList validWriteIdList = new TValidWriteIdList();
    final Iterable<HdfsCachePool> hdfsCachePools = new ArrayList<>();
    final TableParams tableParams = Mockito.mock(TableParams.class);

    Mockito.when(mockProvider.getURI()).thenReturn("getURI");
    Mockito.when(mockProvider.getAuthPolicy()).thenReturn(authPolicy);
    Mockito.when(mockProvider.isReady()).thenReturn(true);
    Mockito.when(mockProvider.loadDb("testDb")).thenReturn(database);
    Mockito.when(mockProvider.loadTable("testDb", "testTable")).thenReturn(tablePair);
    Mockito.when(mockProvider.getTableIfPresent("testDb", "testTable"))
        .thenReturn(tablePair);
    Mockito.when(mockProvider.loadNullPartitionKeyValue())
        .thenReturn("__HIVE_DEFAULT_PARTITION__");
    Mockito.when(mockProvider.loadPartitionList(tableMetaRef)).thenReturn(partitionRefs);
    Mockito.when(mockProvider.loadConstraints(tableMetaRef, msTable))
        .thenReturn(sqlConstraints);
    Mockito.when(mockProvider.loadFunctionNames("testDb")).thenReturn(new ArrayList<>());
    Mockito.when(mockProvider.loadFunction("testDb", "testFunction"))
        .thenReturn(functions);
    Mockito.when(mockProvider.loadDataSources()).thenReturn(dataSources);
    Mockito.when(mockProvider.loadDataSource("testDs")).thenReturn(dataSource);
    Mockito.when(mockProvider.loadPartitionsByRefs(eq(tableMetaRef), anyList(),
        any(ListMap.class), eq(partitionRefs))).thenReturn(partitionMetadataMap);
    Mockito.when(mockProvider.loadTableColumnStatistics(tableMetaRef, new ArrayList<>()))
        .thenReturn(columnStats);
    Mockito.when(mockProvider.loadIcebergTable(tableMetaRef))
        .thenReturn(partialTableInfo);
    Mockito.when(mockProvider.loadIcebergApiTable(tableMetaRef, tableParams, msTable))
        .thenReturn(icebergTable);
    Mockito.when(mockProvider.getValidWriteIdList(tableMetaRef))
        .thenReturn(validWriteIdList);
    Mockito.when(mockProvider.getHdfsCachePools()).thenReturn(hdfsCachePools);

    assertThat(fixture.getURI(), equalTo("getURI"));
    assertThat(fixture.getAuthPolicy(), equalTo(authPolicy));
    assertTrue(fixture.isReady());
    fixture.waitForIsReady(1000L);
    fixture.setIsReady(true);
    assertThat(fixture.loadDb("testDb"), equalTo(database));
    assertThat(fixture.loadTable("testDb", "testTable"), equalTo(tablePair));
    assertThat(fixture.getTableIfPresent("testDb", "testTable"), equalTo(tablePair));
    assertThat(fixture.loadNullPartitionKeyValue(),
        equalTo("__HIVE_DEFAULT_PARTITION__"));
    assertThat(fixture.loadPartitionList(tableMetaRef), equalTo(partitionRefs));
    assertThat(fixture.loadConstraints(tableMetaRef, msTable), equalTo(sqlConstraints));
    assertThat(fixture.loadFunctionNames("testDb"), equalTo(new ArrayList<>()));
    assertThat(fixture.loadFunction("testDb", "testFunction"), equalTo(functions));
    assertThat(fixture.loadDataSources(), equalTo(dataSources));
    assertThat(fixture.loadDataSource("testDs"), equalTo(dataSource));
    assertThat(fixture.loadPartitionsByRefs(tableMetaRef, new ArrayList<>(),
        new ListMap<>(), partitionRefs), equalTo(partitionMetadataMap));
    assertThat(fixture.loadTableColumnStatistics(tableMetaRef, new ArrayList<>()),
        equalTo(columnStats));
    assertThat(fixture.loadIcebergTable(tableMetaRef), equalTo(partialTableInfo));
    assertThat(fixture.loadIcebergApiTable(tableMetaRef, tableParams, msTable),
        equalTo(icebergTable));
    assertThat(fixture.getValidWriteIdList(tableMetaRef), equalTo(validWriteIdList));
    assertThat(fixture.getHdfsCachePools(), equalTo(hdfsCachePools));

    Mockito.verify(mockProvider).getURI();
    Mockito.verify(mockProvider).getAuthPolicy();
    Mockito.verify(mockProvider).isReady();
    Mockito.verify(mockProvider).waitForIsReady(1000L);
    Mockito.verify(mockProvider).setIsReady(true);
    Mockito.verify(mockProvider).loadDb("testDb");
    Mockito.verify(mockProvider).loadTable("testDb", "testTable");
    Mockito.verify(mockProvider).getTableIfPresent("testDb", "testTable");
    Mockito.verify(mockProvider).loadNullPartitionKeyValue();
    Mockito.verify(mockProvider).loadPartitionList(tableMetaRef);
    Mockito.verify(mockProvider).loadConstraints(tableMetaRef, msTable);
    Mockito.verify(mockProvider).loadFunctionNames("testDb");
    Mockito.verify(mockProvider).loadFunction("testDb", "testFunction");
    Mockito.verify(mockProvider).loadDataSources();
    Mockito.verify(mockProvider).loadDataSource("testDs");
    Mockito.verify(mockProvider).loadPartitionsByRefs(eq(tableMetaRef), anyList(),
        any(ListMap.class), eq(partitionRefs));
    Mockito.verify(mockProvider).loadTableColumnStatistics(tableMetaRef,
        new ArrayList<>());
    Mockito.verify(mockProvider).loadIcebergTable(tableMetaRef);
    Mockito.verify(mockProvider).loadIcebergApiTable(tableMetaRef, tableParams, msTable);
    Mockito.verify(mockProvider).getValidWriteIdList(tableMetaRef);
    Mockito.verify(mockProvider).getHdfsCachePools();
    Mockito.verifyNoMoreInteractions(mockProvider);
  }
}
