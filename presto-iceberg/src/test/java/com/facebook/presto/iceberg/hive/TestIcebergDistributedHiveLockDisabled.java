/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.iceberg.HiveTableOperations;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.toIcebergSchema;
import static com.google.common.io.Files.createTempDir;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

@Test
public class TestIcebergDistributedHiveLockDisabled
        extends TestIcebergDistributedHive
{
    public TestIcebergDistributedHiveLockDisabled()
    {
        super(ImmutableMap.of("iceberg.engine.hive.lock-enabled", "false"));
    }

    Table createTable(String tableName, String targetPath, Map<String, String> tableProperties, int columns)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(getDistributedQueryRunner().getDefaultSession().getCatalog().get()).get().getConnectorId();
        ConnectorSession session = getQueryRunner().getDefaultSession().toConnectorSession(connectorId);
        MetastoreContext context = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, session.getWarningCollector(), session.getRuntimeStats());
        HdfsContext hdfsContext = new HdfsContext(session, "tpch", tableName);
        HiveTableOperations operations = new HiveTableOperations(
                getFileHiveMetastore(),
                context,
                getHdfsEnvironment(),
                hdfsContext,
                new IcebergHiveTableOperationsConfig().setLockingEnabled(false),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                "tpch",
                tableName,
                session.getUser(),
                targetPath);
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (int i = 0; i < columns; i++) {
            columnMetadataList.add(ColumnMetadata.builder().setName("column" + i).setType(INTEGER).build());
        }
        TableMetadata metadata = newTableMetadata(
                toIcebergSchema(columnMetadataList),
                PartitionSpec.unpartitioned(), targetPath,
                tableProperties);
        Transaction transaction = createTableTransaction(tableName, operations, metadata);
        transaction.commitTransaction();
        return transaction.table();
    }

    @Test
    public void testCommitTableMetadataForNoLock()
    {
        createTable("iceberg-test-table", createTempDir().toURI().toString(), ImmutableMap.of("engine.hive.lock-enabled", "false"), 2);
        BaseTable table = (BaseTable) loadTable("iceberg-test-table-two");
        createTable("iceberg-test-table-two", createTempDir().toURI().toString(), ImmutableMap.of("engine.hive.lock-enabled", "false"), 3);
        BaseTable tableTwo = (BaseTable) loadTable("iceberg-test-table-two");
        HiveTableOperations operations = (HiveTableOperations) table.operations();
        TableMetadata currentMetadata = operations.current();
        operations.getConfig().setLockingEnabled(false);
        operations.commit(currentMetadata, tableTwo.operations().current());
        assertEquals(operations.current().schema().columns().size(), tableTwo.operations().current().schema().columns().size());
    }

    @Test
    public void testConcurrentCommitsWithNoLock() throws Exception
    {
        int numConcurrentCommits = 10;

        createTable("iceberg-test-table", createTempDir().toURI().toString(), ImmutableMap.of("engine.hive.lock-enabled", "false"), 11);
        BaseTable table = (BaseTable) loadTable("iceberg-test-table");
        for (int i = 0; i < numConcurrentCommits; i++) {
            createTable("iceberg-test-table-" + i, createTempDir().toURI().toString(), ImmutableMap.of("engine.hive.lock-enabled", "false"), i + 1);
        }

        HiveTableOperations operations = (HiveTableOperations) table.operations();
        TableMetadata currentMetadata = operations.current();
        operations.getConfig().setLockingEnabled(false);

        ExecutorService executor = Executors.newFixedThreadPool(numConcurrentCommits);
        try {
            IntStream.range(0, numConcurrentCommits)
                    .forEach(
                            i ->
                                    executor.submit(
                                            () -> {
                                                BaseTable tempTable = (BaseTable) loadTable("iceberg-test-table-" + i);
                                                try {
                                                    operations.commit(currentMetadata, tempTable.operations().current());
                                                }
                                                catch (CommitFailedException e) {
                                                    // failures are expected here
                                                }
                                            }));
        }
        finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        assertNotEquals(operations.current().schema().columns().size(), currentMetadata.schema().columns().size());
    }
}
