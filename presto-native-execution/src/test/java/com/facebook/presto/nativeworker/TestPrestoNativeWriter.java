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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_SCALE_WRITER_THREADS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.SCALE_WRITERS;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.SystemSessionProperties.TASK_PARTITIONED_WRITER_COUNT;
import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.HiveSessionProperties.SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createEmptyTable;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartitionedNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPrestoBenchTables;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoNativeWriter
        extends AbstractTestQueryFramework
{
    private static final String[] TABLE_FORMATS = {"DWRF"};

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createCustomer(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createSupplier(queryRunner);
        createBucketedCustomer(queryRunner);
        createPart(queryRunner);
        createRegion(queryRunner);
        createEmptyTable(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);
        createPrestoBenchTables(queryRunner);
    }

    @Test(groups = {"writer"})
    public void testCreateTableWithUnsupportedFormats()
    {
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        String[] unsupportedTableFormats = {"ORC", "JSON"};
        for (String unsupportedTableFormat : unsupportedTableFormats) {
            assertQueryFails(String.format("CREATE TABLE %s WITH (format = '" + unsupportedTableFormat + "') AS SELECT * FROM nation", tmpTableName), " Unsupported file format in TableWrite: \"" + unsupportedTableFormat + "\".");
        }
    }

    @Test(groups = {"writer"})
    public void testTableFormats()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("scale_writers", "true")
                .setSystemProperty("task_writer_count", "1")
                .setSystemProperty("task_partitioned_writer_count", "2")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "true")
                .setCatalogSessionProperty("hive", "orc_compression_codec", "ZSTD")
                .setCatalogSessionProperty("hive", "compression_codec", "NONE")
                .setCatalogSessionProperty("hive", "hive_storage_format", "PARQUET")
                .setCatalogSessionProperty("hive", "respect_table_format", "false").build();

        for (String tableFormat : TABLE_FORMATS) {
            // TODO add support for presto to query each partition's format then verify written format is correct
            // Partitioned
            //  - Insert
            String tmpTableName = generateRandomTableName();
            try {
                getQueryRunner().execute(session, String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT, nationkey BIGINT) WITH (format = '%s', partitioned_by = ARRAY['regionkey','nationkey'])", tmpTableName, tableFormat));
                // With different storage_format for partition than table format
                getQueryRunner().execute(session, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
                assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT name, regionkey, nationkey FROM nation");
            }
            finally {
                dropTableIfExists(tmpTableName);
            }

            //  - Create
            tmpTableName = generateRandomTableName();
            try {
                getQueryRunner().execute(session, String.format("CREATE TABLE %s WITH (format = '" + tableFormat + "', partitioned_by = ARRAY['comment']) AS SELECT * FROM nation", tmpTableName));
                assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT * FROM nation");
            }
            finally {
                dropTableIfExists(tmpTableName);
            }

            // Unpartitioned
            //  - Insert
            tmpTableName = generateRandomTableName();
            try {
                getQueryRunner().execute(session, String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT, nationkey BIGINT) WITH (format = '%s')", tmpTableName, tableFormat));
                getQueryRunner().execute(session, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
                assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT name, regionkey, nationkey FROM nation");
            }
            finally {
                dropTableIfExists(tmpTableName);
            }

            //  - Create
            tmpTableName = generateRandomTableName();
            try {
                getQueryRunner().execute(session, String.format("CREATE TABLE %s WITH (format = '" + tableFormat + "') AS SELECT * FROM nation", tmpTableName));
                assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT * FROM nation");
            }
            finally {
                dropTableIfExists(tmpTableName);
            }
        }
    }

    @Test(groups = {"writer"})
    public void testCreateUnpartitionedTableAsSelect()
    {
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        Session writeSession = buildSessionForTableWrite();

        try {
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT, nationkey BIGINT) WITH (partitioned_by = ARRAY['regionkey','nationkey'])", tmpTableName));
            // Test insert into an empty table.
            getQueryRunner().execute(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT name, regionkey, nationkey FROM nation");

            // Test failure on insert into existing partitions.
            assertQueryFails(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName),
                    ".*Cannot insert into an existing partition of Hive table: regionkey=.*/nationkey=.*");

            // Test insert into existing partitions if insert_existing_partitions_behavior is set to OVERWRITE.
            Session overwriteSession = Session.builder(writeSession)
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .build();
            getQueryRunner().execute(overwriteSession, String.format("INSERT INTO %s SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test(groups = {"writer"})
    public void testCreatePartitionedTableAsSelect()
    {
        {
            Session session = buildSessionForTableWrite();
            // Generate temporary table name for created partitioned table.
            String partitionedOrdersTableName = generateRandomTableName();

            for (String tableFormat : TABLE_FORMATS) {
                try {
                    getQueryRunner().execute(session, String.format(
                            "CREATE TABLE %s WITH (format = '" + tableFormat + "', " +
                                    "partitioned_by = ARRAY[ 'orderstatus' ]) " +
                                    "AS SELECT custkey, comment, orderstatus FROM orders", partitionedOrdersTableName));
                    assertQuery(String.format("SELECT * FROM %s", partitionedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders");
                }
                finally {
                    dropTableIfExists(partitionedOrdersTableName);
                }
            }
        }
    }

    @Test(groups = {"writer"})
    public void testInsertIntoPartitionedTable()
    {
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        Session writeSession = buildSessionForTableWrite();

        try {
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT, nationkey BIGINT) WITH (partitioned_by = ARRAY['regionkey','nationkey'])", tmpTableName));
            // Test insert into an empty table.
            getQueryRunner().execute(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT name, regionkey, nationkey FROM nation");

            // Test failure on insert into existing partitions.
            assertQueryFails(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName),
                    ".*Cannot insert into an existing partition of Hive table: regionkey=.*/nationkey=.*");

            // Test insert into existing partitions if insert_existing_partitions_behavior is set to OVERWRITE.
            Session overwriteSession = Session.builder(writeSession)
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .build();
            getQueryRunner().execute(overwriteSession, String.format("INSERT INTO %s SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test(groups = {"writer"})
    public void testInsertIntoSpecialPartitionName()
    {
        Session writeSession = buildSessionForTableWrite();
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        try {
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, nationkey VARCHAR) WITH (partitioned_by = ARRAY['nationkey'])", tmpTableName));

            // For special character in partition name, without correct handling, it would throw errors like 'Invalid partition spec: nationkey=A/B'
            // In this test, verify those partition names can be successfully created
            String[] specialCharacters = {"\"", "#", "%", "''", "*", "/", ":", "=", "?", "\\", "\\x7F", "{", "[", "]", "^"}; // escape single quote for sql
            for (String specialCharacter : specialCharacters) {
                getQueryRunner().execute(writeSession, String.format("INSERT INTO %s VALUES ('name', 'A%sB')", tmpTableName, specialCharacter));
                assertQuery(String.format("SELECT nationkey FROM %s", tmpTableName), String.format("VALUES('A%sB')", specialCharacter));
                getQueryRunner().execute(writeSession, String.format("DELETE FROM %s", tmpTableName));
            }
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test(groups = {"writer"})
    public void testCreateBucketTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name for bucketed table.
        String bucketedOrdersTableName = generateRandomTableName();

        for (String tableFormat : TABLE_FORMATS) {
            try {
                getQueryRunner().execute(session, String.format(
                        "CREATE TABLE %s WITH (format = '" + tableFormat + "', " +
                                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                                "bucketed_by = ARRAY[ 'custkey' ], " +
                                "bucket_count = 2) " +
                                "AS SELECT custkey, comment, orderstatus FROM orders", bucketedOrdersTableName));
                assertQuery(String.format("SELECT * FROM %s", bucketedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders");
                assertQuery(String.format("SELECT * FROM %s where \"$bucket\" = 0", bucketedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders where custkey % 2 = 0");
                assertQuery(String.format("SELECT * FROM %s where \"$bucket\" = 1", bucketedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders where custkey % 2 = 1");
            }
            finally {
                dropTableIfExists(bucketedOrdersTableName);
            }
        }
    }

    @Test(groups = {"writer"})
    public void testCreateBucketSortedTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name.
        String badBucketTableName = generateRandomTableName();

        for (String tableFormat : TABLE_FORMATS) {
            try {
                getQueryRunner().execute(session, String.format(
                        "CREATE TABLE %s WITH (format = '%s', " +
                                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                                "bucketed_by=array['custkey'], " +
                                "bucket_count=1, " +
                                "sorted_by=array['orderkey']) " +
                                "AS SELECT custkey, orderkey, orderstatus FROM orders", badBucketTableName, tableFormat));
                assertQueryOrdered(String.format("SELECT custkey, orderkey, orderstatus FROM %s where orderstatus = '0'", badBucketTableName), "SELECT custkey, orderkey, orderstatus FROM orders where orderstatus = '0'");
            }
            finally {
                dropTableIfExists(badBucketTableName);
            }
        }
    }

    @Test(groups = {"writer"})
    public void testScaleWriterTasks()
    {
        Session session = buildSessionForTableWrite();
        String tmpTableName = generateRandomTableName();
        getQueryRunner().execute(session, String.format(
                "CREATE TABLE %s AS SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, CAST(o_orderdate as VARCHAR) as o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM tpchstandard.tiny.orders", tmpTableName));
        assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM " + tmpTableName).getOnlyValue(), 1L);
        dropTableIfExists(tmpTableName);

        tmpTableName = generateRandomTableName();
        getQueryRunner().execute(session, String.format(
                "CREATE TABLE %s AS SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, CAST(o_orderdate as VARCHAR) as o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM tpchstandard.sf100.orders where o_orderdate > Date('1997-01-10')", tmpTableName));
        long files = (long) computeActual("SELECT count(DISTINCT \"$path\") FROM " + tmpTableName).getOnlyValue();
        long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
        assertThat(files).isBetween(2L, workers);
        dropTableIfExists(tmpTableName);
    }

    @Test(groups = {"writer"})
    public void testScaleWriterThreads()
    {
        // no scaling for bucketed tables
        testScaledWriters(
                /*partitioned*/ true,
                /*shufflePartitionedColumns*/ false,
                /*bucketed*/ true,
                /*scaleWriterTasks*/ true,
                /*scaleWriterThreads*/ true,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    ExchangeNode localExchange = (ExchangeNode) tableWriteNode.getSource();
                    assertFalse(localExchange.getPartitioningScheme().isScaleWriters());
                    ExchangeNode remoteExchange = (ExchangeNode) localExchange.getSources().get(0);
                    assertFalse(remoteExchange.getPartitioningScheme().isScaleWriters());
                });

        // no scaling when shuffle_partitioned_columns_for_table_write is requested
        testScaledWriters(
                /*partitioned*/ true,
                /*shufflePartitionedColumns*/ true,
                /*bucketed*/ false,
                /*scaleWriterTasks*/ true,
                /*scaleWriterThreads*/ true,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    ExchangeNode localExchange = (ExchangeNode) tableWriteNode.getSource();
                    assertFalse(localExchange.getPartitioningScheme().isScaleWriters());
                    ExchangeNode remoteExchange = (ExchangeNode) localExchange.getSources().get(0);
                    assertFalse(remoteExchange.getPartitioningScheme().isScaleWriters());
                });

        // scale tasks and threads for partitioned tables if enabled
        testScaledWriters(
                /*partitioned*/ true,
                /*shufflePartitionedColumns*/ false,
                /*bucketed*/ false,
                /*scaleWriterTasks*/ true,
                /*scaleWriterThreads*/ true,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    ExchangeNode localExchange = (ExchangeNode) tableWriteNode.getSource();
                    assertTrue(localExchange.getPartitioningScheme().isScaleWriters());
                    assertThat(localExchange.getPartitioningScheme().getPartitioning().getArguments())
                            // single partitioning column
                            .hasSize(1);
                    assertThat(localExchange.getPartitioningScheme().getPartitioning().getHandle().getConnectorId())
                            .isPresent();
                    ExchangeNode remoteExchange = (ExchangeNode) localExchange.getSources().get(0);
                    assertEquals(remoteExchange.getPartitioningScheme().getPartitioning().getHandle(), SCALED_WRITER_DISTRIBUTION);
                });
        testScaledWriters(
                /*partitioned*/ true,
                /*shufflePartitionedColumns*/ false,
                /*bucketed*/ false,
                /*scaleWriterTasks*/ true,
                /*scaleWriterThreads*/ false,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    ExchangeNode localExchange = (ExchangeNode) tableWriteNode.getSource();
                    assertFalse(localExchange.getPartitioningScheme().isScaleWriters());
                    assertEquals(localExchange.getPartitioningScheme().getPartitioning().getHandle(), FIXED_ARBITRARY_DISTRIBUTION);
                    ExchangeNode remoteExchange = (ExchangeNode) localExchange.getSources().get(0);
                    assertEquals(remoteExchange.getPartitioningScheme().getPartitioning().getHandle(), SCALED_WRITER_DISTRIBUTION);
                });
        testScaledWriters(
                /*partitioned*/ true,
                /*shufflePartitionedColumns*/ false,
                /*bucketed*/ false,
                /*scaleWriterTasks*/ false,
                /*scaleWriterThreads*/ true,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    ExchangeNode localExchange = (ExchangeNode) tableWriteNode.getSource();
                    assertTrue(localExchange.getPartitioningScheme().isScaleWriters());
                    assertThat(localExchange.getPartitioningScheme().getPartitioning().getArguments())
                            // single partitioning column
                            .hasSize(1);
                });
        testScaledWriters(
                /*partitioned*/ true,
                /*shufflePartitionedColumns*/ false,
                /*bucketed*/ false,
                /*scaleWriterTasks*/ false,
                /*scaleWriterThreads*/ false,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    ExchangeNode localExchange = (ExchangeNode) tableWriteNode.getSource();
                    assertFalse(localExchange.getPartitioningScheme().isScaleWriters());
                    assertEquals(localExchange.getPartitioningScheme().getPartitioning().getHandle(), FIXED_ARBITRARY_DISTRIBUTION);
                });

        // scale thread writers for non partitioned tables
        testScaledWriters(
                /*partitioned*/ false,
                /*shufflePartitionedColumns*/ false,
                /*bucketed*/ false,
                /*scaleWriterTasks*/ true,
                /*scaleWriterThreads*/ true,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    ExchangeNode localExchange = (ExchangeNode) tableWriteNode.getSource();
                    assertTrue(localExchange.getPartitioningScheme().isScaleWriters());
                    assertEquals(localExchange.getPartitioningScheme().getPartitioning().getHandle(), FIXED_ARBITRARY_DISTRIBUTION);
                    ExchangeNode remoteExchange = (ExchangeNode) localExchange.getSources().get(0);
                    assertEquals(remoteExchange.getPartitioningScheme().getPartitioning().getHandle(), SCALED_WRITER_DISTRIBUTION);
                });
        // scale writers disabled and task concurrency is equal to writer count
        testScaledWriters(
                /*partitioned*/ false,
                /*shufflePartitionedColumns*/ false,
                /*bucketed*/ false,
                /*scaleWriterTasks*/ false,
                /*scaleWriterThreads*/ false,
                4,
                8,
                4,
                plan -> {
                    TableWriterNode tableWriteNode = searchFrom(plan.getRoot())
                            .where(node -> node instanceof TableWriterNode)
                            .findOnlyElement();
                    // no exchanges expected
                    assertThat(tableWriteNode.getSource()).isInstanceOf(TableScanNode.class);
                });
    }

    private void testScaledWriters(
            boolean partitioned,
            boolean shufflePartitionedColumns,
            boolean bucketed,
            boolean scaleWriterTasks,
            boolean scaleWriterThreads,
            Consumer<Plan> planAssertion)
    {
        testScaledWriters(
                partitioned,
                shufflePartitionedColumns,
                bucketed,
                scaleWriterTasks,
                scaleWriterThreads,
                4,
                8,
                16,
                planAssertion);
    }

    private void testScaledWriters(
            boolean partitioned,
            boolean shufflePartitionedColumns,
            boolean bucketed,
            boolean scaleWriterTasks,
            boolean scaleWriterThreads,
            int writerCount,
            int partitionedWriterCount,
            int taskConcurrency,
            Consumer<Plan> planAssertion)
    {
        String tableName = generateRandomTableName();
        OptionalLong bucketCount = OptionalLong.empty();
        long partitionCount = 1;

        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("orderkey", "BIGINT");
        columns.put("custkey", "BIGINT");
        columns.put("orderstatus", "VARCHAR");

        List<String> properties = new ArrayList<>();
        properties.add("format = 'DWRF'");
        if (partitioned) {
            properties.add("partitioned_by = ARRAY['orderstatus']");
            partitionCount = 3;
        }
        if (bucketed) {
            properties.add("bucketed_by = ARRAY['orderkey']");
            bucketCount = OptionalLong.of(3);
            properties.add("bucket_count = " + bucketCount.getAsLong());
        }

        Session session = Session.builder(getSession())
                .setSystemProperty(SCALE_WRITERS, scaleWriterTasks + "")
                .setSystemProperty(NATIVE_EXECUTION_SCALE_WRITER_THREADS_ENABLED, scaleWriterThreads + "")
                .setSystemProperty(TASK_WRITER_COUNT, writerCount + "")
                .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, partitionedWriterCount + "")
                .setSystemProperty(TASK_CONCURRENCY, taskConcurrency + "")
                .setCatalogSessionProperty("hive", SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE, shufflePartitionedColumns + "")
                .build();

        String createTable = format(
                "CREATE TABLE %s (%s) WITH (%s)",
                tableName,
                Joiner.on(", ")
                        .withKeyValueSeparator(" ")
                        .join(columns),
                Joiner.on(", ").join(properties));
        assertUpdate(createTable);

        long numberOfRowsInOrdersTable = (long) getQueryRunner().execute("SELECT count(*) FROM orders").getOnlyValue();

        String insert = format(
                "INSERT INTO %s SELECT %s FROM orders",
                tableName,
                Joiner.on(", ").join(columns.keySet()));
        assertUpdate(session, insert, numberOfRowsInOrdersTable, planAssertion);
        assertQuery(
                format(
                        "SELECT %s FROM %s",
                        Joiner.on(", ").join(columns.keySet()),
                        tableName),
                format(
                        "SELECT %s FROM orders",
                        Joiner.on(", ").join(columns.keySet())));
        assertFileCount(tableName, partitionCount, bucketCount);

        assertUpdate(format("DROP TABLE %s", tableName));

        tableName = generateRandomTableName();

        String ctas = format(
                "CREATE TABLE %s WITH (%s) AS SELECT %s FROM orders",
                tableName,
                Joiner.on(", ").join(properties),
                Joiner.on(", ").join(columns.keySet()));
        assertUpdate(session, ctas, numberOfRowsInOrdersTable, planAssertion);
        assertQuery(
                format(
                        "SELECT %s FROM %s",
                        Joiner.on(", ").join(columns.keySet()),
                        tableName),
                format(
                        "SELECT %s FROM orders",
                        Joiner.on(", ").join(columns.keySet())));
        assertFileCount(tableName, partitionCount, bucketCount);

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    private void assertFileCount(String tableName, long partitionCount, OptionalLong bucketCount)
    {
        long actual = (long) computeActual(format("SELECT count(DISTINCT \"$path\") FROM %s", tableName)).getOnlyValue();
        if (bucketCount.isPresent()) {
            assertEquals(actual, bucketCount.getAsLong() * partitionCount);
        }
        else {
            assertThat(actual).isGreaterThanOrEqualTo(partitionCount);
        }
    }

    @Test(groups = {"writer"})
    public void testCollectColumnStatisticsOnCreateTable()
    {
        Session session = buildSessionForTableWrite();
        String tmpTableName = generateRandomTableName();
        assertUpdate(session, format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ") " +
                "AS " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), to_ieee754_64(1), sequence(0, 10), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), to_ieee754_64(2), sequence(10, 20), 'p1')," +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), to_ieee754_64(3), sequence(20, 25), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), to_ieee754_64(4), sequence(30, 35), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar)", tmpTableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8.0
                        "('c_varbinary', 24.0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 184.0E0, null, 0.5, null, null, null, null), " + // 176
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (column_name, data_size, distinct_values_count, nulls_fraction, row_count, low_value, high_value, histogram)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8
                        "('c_varbinary', 24.0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 104.0E0, null, 0.5, null, null, null, null), " + // 96
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (column_name, data_size, distinct_values_count, nulls_fraction, row_count, low_value, high_value, histogram)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_array', null, 0E0, 0E0, null, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null, null)) AS x (column_name, data_size, distinct_values_count, nulls_fraction, row_count, low_value, high_value, histogram)");

        dropTableIfExists(tmpTableName);
    }

    @Test(groups = {"writer"})
    public void testCollectColumnStatisticsOnInsert()
    {
        Session session = buildSessionForTableWrite();
        String tmpTableName = generateRandomTableName();
        assertUpdate(session, format("" +
                "CREATE TABLE %s ( " +
                "   c_boolean BOOLEAN, " +
                "   c_bigint BIGINT, " +
                "   c_double DOUBLE, " +
                "   c_timestamp TIMESTAMP, " +
                "   c_varchar VARCHAR, " +
                "   c_varbinary VARBINARY, " +
                "   c_array ARRAY(BIGINT), " +
                "   p_varchar VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ")", tmpTableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, null,  'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), to_ieee754_64(1), sequence(0, 10), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), to_ieee754_64(2), sequence(10, 20), 'p1')," +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), to_ieee754_64(3), sequence(20, 25), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), to_ieee754_64(4), sequence(30, 35), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar)", tmpTableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8
                        "('c_varbinary', 24.0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 184.0E0, null, 0.5E0, null, null, null, null), " + // 176
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (column_name, data_size, distinct_values_count, nulls_fraction, row_count, low_value, high_value, histogram)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8
                        "('c_varbinary', 24.0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 104.0E0, null, 0.5, null, null, null, null), " + // 96
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (column_name, data_size, distinct_values_count, nulls_fraction, row_count, low_value, high_value, histogram)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_array', null, 0E0, 0E0, null, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null, null)) AS x (column_name, data_size, distinct_values_count, nulls_fraction, row_count, low_value, high_value, histogram)");

        dropTableIfExists(tmpTableName);
    }

    private void dropTableIfExists(String tableName)
    {
        computeExpected(String.format("DROP TABLE IF EXISTS %s", tableName), ImmutableList.of(BIGINT));
    }

    private String generateRandomTableName()
    {
        String tableName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
        // Clean up if the temporary named table already exists.
        dropTableIfExists(tableName);
        return tableName;
    }

    private Session buildSessionForTableWrite()
    {
        return Session.builder(getSession())
                .setSystemProperty("scale_writers", "true")
                .setSystemProperty("task_writer_count", "1")
                .setSystemProperty("task_partitioned_writer_count", "2")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "true")
                .setCatalogSessionProperty("hive", "orc_compression_codec", "ZSTD")
                .build();
    }
}
