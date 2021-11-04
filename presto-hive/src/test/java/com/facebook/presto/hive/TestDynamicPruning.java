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

import com.facebook.airlift.testing.TempFile;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.CacheQuotaRequirement.NO_CACHE_REQUIREMENT;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveBatchPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveSelectivePageSourceFactories;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.TestHivePageSink.getColumnHandles;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDynamicPruning
{
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";
    private static final Column BUCKET_COLUMN = new Column("l_orderkey", HIVE_INT, Optional.empty());
    private static final Column PARTITION_COLUMN = new Column("ds", HIVE_STRING, Optional.empty());
    private static final HiveColumnHandle PARTITION_HIVE_COLUMN_HANDLE = new HiveColumnHandle(
            "ds",
            HIVE_STRING,
            TypeSignature.parseTypeSignature("varchar"),
            1,
            HiveColumnHandle.ColumnType.PARTITION_KEY,
            Optional.empty(),
            ImmutableList.of(),
            Optional.empty());

    @Test
    public void testDynamicBucketPruning()
    {
        HiveClientConfig config = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle();
        try (TempFile tempFile = new TempFile()) {
            ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, config, new SplitContext(false, getToSkipTupleDomainForPartition()), metastoreClientConfig, tempFile.file());
            assertEquals(emptyPageSource.getClass(), HiveEmptySplitPageSource.class);

            ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, config, new SplitContext(false, getToKeepTupleDomainForPartition()), metastoreClientConfig, tempFile.file());
            assertEquals(nonEmptyPageSource.getClass(), HivePageSource.class);
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testDynamicPartitionPruning()
    {
        HiveClientConfig config = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle();
        try (TempFile tempFile = new TempFile()) {
            ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, config, new SplitContext(false, getToSkipTupleDomain()), metastoreClientConfig, tempFile.file());
            assertEquals(emptyPageSource.getClass(), HiveEmptySplitPageSource.class);

            ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, config, new SplitContext(false, getToKeepTupleDomain()), metastoreClientConfig, tempFile.file());
            assertEquals(nonEmptyPageSource.getClass(), HivePageSource.class);
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    private static ConnectorPageSource createTestingPageSource(HiveTransactionHandle transaction, HiveClientConfig config, SplitContext splitContext, MetastoreClientConfig metastoreClientConfig, File outputFile)
    {
        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey(PARTITION_COLUMN.getName(), Optional.of("2020-09-09")));
        Map<Integer, Column> partitionSchemaDifference = ImmutableMap.of(1, new Column("ds", HIVE_STRING, Optional.empty()));
        HiveSplit split = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                "",
                "file:///" + outputFile.getAbsolutePath(),
                0,
                outputFile.length(),
                outputFile.length(),
                Instant.now().toEpochMilli(),
                new Storage(
                        StorageFormat.create(config.getHiveStorageFormat().getSerDe(), config.getHiveStorageFormat().getInputFormat(), config.getHiveStorageFormat().getOutputFormat()),
                        "location",
                        Optional.of(new HiveBucketProperty(ImmutableList.of("l_orderkey"), 10, ImmutableList.of(), HIVE_COMPATIBLE, Optional.empty())),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                partitionKeys,
                ImmutableList.of(),
                OptionalInt.of(1),
                OptionalInt.of(1),
                NO_PREFERENCE,
                getColumnHandles().size(),
                TableToPartitionMapping.mapColumnsByIndex(partitionSchemaDifference),
                Optional.empty(),
                false,
                Optional.empty(),
                NO_CACHE_REQUIREMENT,
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of());

        TableHandle tableHandle = new TableHandle(
                new ConnectorId(HIVE_CATALOG),
                new HiveTableHandle(SCHEMA_NAME, TABLE_NAME),
                transaction,
                Optional.of(new HiveTableLayoutHandle(
                        new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                        "path",
                        ImmutableList.of(PARTITION_HIVE_COLUMN_HANDLE),
                        getColumnHandles().stream()
                                .map(column -> new Column(column.getName(), column.getHiveType(), Optional.empty()))
                                .collect(toImmutableList()),
                        ImmutableMap.of(),
                        TupleDomain.all(),
                        TRUE_CONSTANT,
                        ImmutableMap.of(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        "layout",
                        Optional.empty(),
                        false)));
        HivePageSourceProvider provider = new HivePageSourceProvider(config, createTestHdfsEnvironment(config, metastoreClientConfig), getDefaultHiveRecordCursorProvider(config, metastoreClientConfig), getDefaultHiveBatchPageSourceFactories(config, metastoreClientConfig), getDefaultHiveSelectivePageSourceFactories(config, metastoreClientConfig), FUNCTION_AND_TYPE_MANAGER, ROW_EXPRESSION_SERVICE);
        return provider.createPageSource(transaction, getSession(config), split, tableHandle.getLayout().get(), ImmutableList.copyOf(getColumnHandles()), splitContext);
    }

    private static TupleDomain<ColumnHandle> getToSkipTupleDomain()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new HiveColumnHandle(
                                BUCKET_COLUMN.getName(),
                                BUCKET_COLUMN.getType(),
                                parseTypeSignature(StandardTypes.VARCHAR),
                                0,
                                REGULAR,
                                Optional.empty(),
                                Optional.empty()),
                        Domain.singleValue(INTEGER, 10L)));
    }

    private TupleDomain<ColumnHandle> getToSkipTupleDomainForPartition()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new HiveColumnHandle(
                                PARTITION_COLUMN.getName(),
                                PARTITION_COLUMN.getType(),
                                parseTypeSignature(StandardTypes.VARCHAR),
                                1,
                                PARTITION_KEY,
                                Optional.empty(),
                                Optional.empty()),
                        singleValue(createVarcharType(15), utf8Slice("2020-09-08"))));
    }

    private static TupleDomain<ColumnHandle> getToKeepTupleDomain()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new HiveColumnHandle(
                                BUCKET_COLUMN.getName(),
                                BUCKET_COLUMN.getType(),
                                parseTypeSignature(StandardTypes.VARCHAR),
                                0,
                                REGULAR,
                                Optional.empty(),
                                Optional.empty()),
                        Domain.singleValue(INTEGER, 1L)));
    }

    private TupleDomain<ColumnHandle> getToKeepTupleDomainForPartition()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        new HiveColumnHandle(
                                PARTITION_COLUMN.getName(),
                                PARTITION_COLUMN.getType(),
                                parseTypeSignature(StandardTypes.VARCHAR),
                                1,
                                PARTITION_KEY,
                                Optional.empty(),
                                Optional.empty()),
                        singleValue(createVarcharType(15), utf8Slice("2020-09-09"))));
    }

    private static TestingConnectorSession getSession(HiveClientConfig config)
    {
        return new TestingConnectorSession(new HiveSessionProperties(config, new OrcFileWriterConfig(), new ParquetFileWriterConfig(), new CacheConfig()).getSessionProperties());
    }
}
