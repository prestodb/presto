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

package com.facebook.presto.hive;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.MAX_PARTITION_KEY_COLUMN_INDEX;
import static com.facebook.presto.hive.HiveColumnHandle.bucketColumnHandle;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHivePartitionManager
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    private static final String USER_NAME = "user";
    private static final String LOCATION = "somewhere/over/the/rainbow";
    private static final Column PARTITION_COLUMN = new Column("ds", HIVE_STRING, Optional.empty());
    private static final Column BUCKET_COLUMN = new Column("c1", HIVE_INT, Optional.empty());
    private static final Table TABLE = new Table(
            SCHEMA_NAME,
            TABLE_NAME,
            USER_NAME,
            PrestoTableType.MANAGED_TABLE,
            new Storage(fromHiveStorageFormat(ORC),
                    LOCATION,
                    Optional.of(new HiveBucketProperty(
                            ImmutableList.of(BUCKET_COLUMN.getName()),
                            100,
                            ImmutableList.of(),
                            HIVE_COMPATIBLE,
                            Optional.empty())),
                    false,
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            ImmutableList.of(BUCKET_COLUMN),
            ImmutableList.of(PARTITION_COLUMN),
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty());

    private static final List<String> PARTITIONS = ImmutableList.of("ds=2019-07-23", "ds=2019-08-23");

    private HivePartitionManager hivePartitionManager = new HivePartitionManager(new TestingTypeManager(), new HiveClientConfig());
    private final TestingSemiTransactionalHiveMetastore metastore = TestingSemiTransactionalHiveMetastore.create();

    @BeforeClass
    public void setUp()
    {
        metastore.addTable(SCHEMA_NAME, TABLE_NAME, TABLE, PARTITIONS);
    }

    @Test
    public void testUsesBucketingIfSmallEnough()
    {
        HiveTableHandle tableHandle = new HiveTableHandle(SCHEMA_NAME, TABLE_NAME);
        HivePartitionResult result = hivePartitionManager.getPartitions(
                metastore,
                tableHandle,
                Constraint.alwaysTrue(),
                new TestingConnectorSession(
                        new HiveSessionProperties(
                                new HiveClientConfig(),
                                new OrcFileWriterConfig(),
                                new ParquetFileWriterConfig(),
                                new CacheConfig()).getSessionProperties()));
        assertTrue(result.getBucketHandle().isPresent(), "bucketHandle is not present");
        assertFalse(result.getBucketFilter().isPresent(), "bucketFilter is present");
    }

    @Test
    public void testIgnoresBucketingWhenTooManyBuckets()
    {
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setMaxBucketsForGroupedExecution(100),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig())
                        .getSessionProperties());
        HivePartitionResult result = hivePartitionManager.getPartitions(metastore, new HiveTableHandle(SCHEMA_NAME, TABLE_NAME), Constraint.alwaysTrue(), session);
        assertFalse(result.getBucketHandle().isPresent(), "bucketHandle is present");
        assertFalse(result.getBucketFilter().isPresent(), "bucketFilter is present");
    }

    @Test
    public void testUsesBucketingWithPartitionFilters()
    {
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setMaxBucketsForGroupedExecution(100),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        HiveTableHandle tableHandle = new HiveTableHandle(SCHEMA_NAME, TABLE_NAME);
        HivePartitionResult result = hivePartitionManager.getPartitions(
                metastore,
                tableHandle,
                new Constraint<>(TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                new HiveColumnHandle(
                                        PARTITION_COLUMN.getName(),
                                        PARTITION_COLUMN.getType(),
                                        parseTypeSignature(StandardTypes.VARCHAR),
                                        MAX_PARTITION_KEY_COLUMN_INDEX,
                                        PARTITION_KEY,
                                        Optional.empty(),
                                        Optional.empty()),
                                Domain.singleValue(VARCHAR, utf8Slice("2019-07-23"))))),
                session);
        assertTrue(result.getBucketHandle().isPresent(), "bucketHandle is not present");
        assertFalse(result.getBucketFilter().isPresent(), "bucketFilter is present");
    }

    @Test
    public void testUsesBucketingWithBucketFilters()
    {
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setMaxBucketsForGroupedExecution(100),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        HiveTableHandle tableHandle = new HiveTableHandle(SCHEMA_NAME, TABLE_NAME);
        HivePartitionResult result = hivePartitionManager.getPartitions(
                metastore,
                tableHandle,
                new Constraint<>(TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                new HiveColumnHandle(
                                        BUCKET_COLUMN.getName(),
                                        BUCKET_COLUMN.getType(),
                                        parseTypeSignature(StandardTypes.VARCHAR),
                                        0,
                                        REGULAR,
                                        Optional.empty(),
                                        Optional.empty()),
                                Domain.singleValue(INTEGER, 1L)))),
                session);
        assertTrue(result.getBucketHandle().isPresent(), "bucketHandle is not present");
        assertTrue(result.getBucketFilter().isPresent(), "bucketFilter is present");
    }

    @Test
    public void testUsesBucketingWithBucketColumn()
    {
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setMaxBucketsForGroupedExecution(1),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());
        HiveTableHandle tableHandle = new HiveTableHandle(SCHEMA_NAME, TABLE_NAME);
        HivePartitionResult result = hivePartitionManager.getPartitions(
                metastore,
                tableHandle,
                new Constraint<>(TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                bucketColumnHandle(),
                                Domain.singleValue(INTEGER, 1L)))),
                session);
        assertTrue(result.getBucketHandle().isPresent(), "bucketHandle is not present");
        assertTrue(result.getBucketFilter().isPresent(), "bucketFilter is present");
    }

    @Test
    public void testIgnoresBucketingWhenConfigured()
    {
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setIgnoreTableBucketing(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig())
                        .getSessionProperties());
        HivePartitionResult result = hivePartitionManager.getPartitions(metastore, new HiveTableHandle(SCHEMA_NAME, TABLE_NAME), Constraint.alwaysTrue(), session);
        assertFalse(result.getBucketHandle().isPresent(), "bucketHandle is present");
        assertFalse(result.getBucketFilter().isPresent(), "bucketFilter is present");
    }
}
