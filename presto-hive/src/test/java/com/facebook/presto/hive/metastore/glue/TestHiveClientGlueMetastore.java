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
package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.TableInput;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.AbstractTestHiveClientLocal;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.hive.HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.DELTA_LAKE_PROVIDER;
import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.SPARK_TABLE_PROVIDER_KEY;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionNamesWithEmptyVersion;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isDeltaLakeTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isIcebergTable;
import static com.facebook.presto.hive.metastore.glue.PartitionFilterBuilder.DECIMAL_TYPE;
import static com.facebook.presto.hive.metastore.glue.PartitionFilterBuilder.decimalOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveClientGlueMetastore
        extends AbstractTestHiveClientLocal
{
    private ExecutorService executorService;

    private static final String PARTITION_KEY = "part_key_1";
    private static final String PARTITION_KEY2 = "part_key_2";
    private static final String TEST_DATABASE_NAME_PREFIX = "test_glue";
    private static final TypeTranslator HIVE_TYPE_TRANSLATOR = new HiveTypeTranslator();

    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("id", BIGINT))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_VARCHAR = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, VARCHAR))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_TWO_KEYS = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, VARCHAR))
            .add(new ColumnMetadata(PARTITION_KEY2, BIGINT))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_TINYINT = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, TinyintType.TINYINT))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_SMALLINT = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, SmallintType.SMALLINT))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_INTEGER = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, IntegerType.INTEGER))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_BIGINT = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, BIGINT))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_DECIMAL = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, DECIMAL_TYPE))
            .build();
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED_DATE = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata(PARTITION_KEY, DateType.DATE))
            .build();
    private static final List<String> VARCHAR_PARTITION_VALUES = ImmutableList.of("2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01");

    public TestHiveClientGlueMetastore()
    {
        super(TEST_DATABASE_NAME_PREFIX + randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
    }

    @BeforeClass
    public void setUp()
    {
        executorService = newCachedThreadPool(daemonThreadsNamed("hive-glue-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    /**
     * GlueHiveMetastore currently uses AWS Default Credential Provider Chain,
     * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
     * on ways to set your AWS credentials which will be needed to run this test.
     */
    @Override
    protected ExtendedHiveMetastore createMetastore(File tempDir)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of(), hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig();
        glueConfig.setDefaultWarehouseDir(tempDir.toURI().toString());

        return new GlueHiveMetastore(hdfsEnvironment, glueConfig, executor);
    }

    @Override
    protected Set<HiveStorageFormat> getSupportedCreateTableHiveStorageFormats()
    {
        return difference(
                ImmutableSet.copyOf(super.getSupportedCreateTableHiveStorageFormats()),
                // Exclude PAGEFILE because Glue catalog requires a non-empty SerDe and PageFile has it set to an
                // empty string.
                ImmutableSet.of(PAGEFILE));
    }

    @Override
    public void testRenameTable()
    {
        // rename table is not yet supported by Glue
    }

    @Override
    public void testPartitionStatisticsSampling()
            throws Exception
    {
        // Glue metastore does not support column level statistics
    }

    @Override
    public void testUpdateTableColumnStatistics()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdatePartitionColumnStatistics()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testTableConstraints()
    {
        // GlueMetastore has no support for table constraints
    }

    @Override
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, BASIC_STATISTICS_1, BASIC_STATISTICS_2, BASIC_STATISTICS_1, EMPTY_TABLE_STATISTICS);
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("get_partitions");
        try {
            createDummyPartitionedTable(tablePartitionFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
            Table table = getMetastoreClient().getTable(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName()).get();
            Optional<List<PartitionNameWithVersion>> partitionNames = getMetastoreClient().getPartitionNames(METASTORE_CONTEXT, table.getDatabaseName(), table.getTableName());
            assertTrue(partitionNames.isPresent());
            assertEquals(partitionNames.get(), getPartitionNamesWithEmptyVersion(ImmutableList.of("ds=2016-01-01", "ds=2016-01-02")));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testTableWithoutStorageDescriptor()
    {
        // StorageDescriptor is an Optional field for Glue tables. Iceberg and Delta Lake tables may not have it set.
        SchemaTableName table = temporaryTable("test_missing_storage_descriptor");
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(table.getSchemaName())
                .withName(table.getTableName());
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        try {
            ConnectorSession session = newSession();
            MetastoreContext metastoreContext = new MetastoreContext(
                    session.getIdentity(),
                    session.getQueryId(),
                    session.getClientInfo(),
                    session.getClientTags(),
                    session.getSource(),
                    getMetastoreHeaders(session),
                    false,
                    DEFAULT_COLUMN_CONVERTER_PROVIDER,
                    session.getWarningCollector(),
                    session.getRuntimeStats());
            TableInput tableInput = new TableInput()
                    .withName(table.getTableName())
                    .withTableType(EXTERNAL_TABLE.name());
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));

            assertThatThrownBy(() -> getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()))
                    .hasMessageStartingWith("Table StorageDescriptor is null for table");
            glueClient.deleteTable(deleteTableRequest);

            // Iceberg table
            tableInput = tableInput.withParameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE));
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));
            assertTrue(isIcebergTable(getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()).orElseThrow(() -> new NoSuchElementException())));
            glueClient.deleteTable(deleteTableRequest);

            // Delta Lake table
            tableInput = tableInput.withParameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER));
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));
            assertTrue(isDeltaLakeTable(getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()).orElseThrow(() -> new NoSuchElementException())));
        }
        finally {
            // Table cannot be dropped through HiveMetastore since a TableHandle cannot be created
            glueClient.deleteTable(new DeleteTableRequest()
                    .withDatabaseName(table.getSchemaName())
                    .withName(table.getTableName()));
        }
    }

    @Test
    public void testGetPartitionsWithFilterUsingReservedKeywordsAsColumnName()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("get_partitions_with_filter_using_reserved_keyword_column_name");
        try {
            String reservedKeywordPartitionColumnName = "key";
            String regularColumnPartitionName = "int_partition";
            List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata("t_string", createUnboundedVarcharType()))
                    .add(new ColumnMetadata(reservedKeywordPartitionColumnName, createUnboundedVarcharType()))
                    .add(new ColumnMetadata(regularColumnPartitionName, BIGINT))
                    .build();
            List<String> partitionedBy = ImmutableList.of(reservedKeywordPartitionColumnName, regularColumnPartitionName);

            doCreateEmptyTable(tableName, ORC, columns, partitionedBy);

            ExtendedHiveMetastore metastoreClient = getMetastoreClient();
            Table table = metastoreClient.getTable(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(tableName));

            String partitionName1 = makePartName(ImmutableList.of(reservedKeywordPartitionColumnName, regularColumnPartitionName), ImmutableList.of("value1", "1"));
            String partitionName2 = makePartName(ImmutableList.of(reservedKeywordPartitionColumnName, regularColumnPartitionName), ImmutableList.of("value2", "2"));

            List<PartitionWithStatistics> partitions = ImmutableList.of(partitionName1, partitionName2)
                    .stream()
                    .map(partitionName -> new PartitionWithStatistics(createDummyPartition(table, partitionName), partitionName, PartitionStatistics.empty()))
                    .collect(toImmutableList());
            metastoreClient.addPartitions(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), partitions);
            metastoreClient.updatePartitionStatistics(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), partitionName1, currentStatistics -> EMPTY_TABLE_STATISTICS);
            metastoreClient.updatePartitionStatistics(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), partitionName2, currentStatistics -> EMPTY_TABLE_STATISTICS);

            Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                    .addStringValues(reservedKeywordPartitionColumnName, "value1")
                    .addBigintValues(regularColumnPartitionName, 2L)
                    .build();

            List<PartitionNameWithVersion> partitionNames = metastoreClient.getPartitionNamesByFilter(
                    METASTORE_CONTEXT,
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    predicates);

            assertFalse(partitionNames.isEmpty());
            assertEquals(partitionNames, ImmutableList.of("key=value2/int_partition=2"));

            // KEY is a reserved keyword in the grammar of the SQL parser used internally by Glue API
            // and therefore should not be used in the partition filter
            predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                    .addStringValues(reservedKeywordPartitionColumnName, "value1")
                    .build();

            partitionNames = metastoreClient.getPartitionNamesByFilter(
                    METASTORE_CONTEXT,
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    predicates);
            assertFalse(partitionNames.isEmpty());
            assertEquals(partitionNames, ImmutableList.of("key=value1/int_partition=1", "key=value2/int_partition=2"));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testGetPartitionsFilterVarChar()
            throws Exception
    {
        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues(PARTITION_KEY, "2020-01-01")
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(VARCHAR, utf8Slice("2020-02-01")))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(VARCHAR, utf8Slice("2020-02-01"), true, utf8Slice("2020-03-01"), true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(VARCHAR, utf8Slice("2020-03-01")))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues(PARTITION_KEY, "2020-01-01", "2020-02-01")
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(VARCHAR, utf8Slice("2020-03-01")))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_VARCHAR,
                PARTITION_KEY,
                VARCHAR_PARTITION_VALUES,
                ImmutableList.of(singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
                        ImmutableList.of("2020-01-01"),
                        ImmutableList.of("2020-03-01", "2020-04-01"),
                        ImmutableList.of("2020-02-01", "2020-03-01"),
                        ImmutableList.of("2020-03-01", "2020-04-01"),
                        ImmutableList.of("2020-01-01", "2020-02-01"),
                        ImmutableList.of("2020-01-01", "2020-02-01"),
                        ImmutableList.of("2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01")));
    }

    @Test
    public void testGetPartitionsFilterBigInt()
            throws Exception
    {
        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addBigintValues(PARTITION_KEY, 1000L)
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(BIGINT, 100L))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(BIGINT, 100L, true, 1000L, true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(BIGINT, 100L))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addBigintValues(PARTITION_KEY, 1L, 1000000L)
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(BIGINT, 1000L))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_BIGINT,
                PARTITION_KEY,
                ImmutableList.of("1", "100", "1000", "1000000"),
                ImmutableList.of(singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
                        ImmutableList.of("1000"),
                        ImmutableList.of("1000", "1000000"),
                        ImmutableList.of("100", "1000"),
                        ImmutableList.of("100", "1000", "1000000"),
                        ImmutableList.of("1", "1000000"),
                        ImmutableList.of("1", "100"),
                        ImmutableList.of("1", "100", "1000", "1000000")));
    }

    @Test
    public void testGetPartitionsFilterInteger()
            throws Exception
    {
        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addIntegerValues(PARTITION_KEY, 1000L)
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(IntegerType.INTEGER, 100L))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(IntegerType.INTEGER, 100L, true, 1000L, true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(IntegerType.INTEGER, 100L))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addIntegerValues(PARTITION_KEY, 1L, 1000000L)
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(IntegerType.INTEGER, 1000L))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_INTEGER,
                PARTITION_KEY,
                ImmutableList.of("1", "100", "1000", "1000000"),
                ImmutableList.of(singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
                        ImmutableList.of("1000"),
                        ImmutableList.of("1000", "1000000"),
                        ImmutableList.of("100", "1000"),
                        ImmutableList.of("100", "1000", "1000000"),
                        ImmutableList.of("1", "1000000"),
                        ImmutableList.of("1", "100"),
                        ImmutableList.of("1", "100", "1000", "1000000")));
    }

    @Test
    public void testGetPartitionsFilterSmallInt()
            throws Exception
    {
        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addSmallintValues(PARTITION_KEY, 1000L)
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(SmallintType.SMALLINT, 100L))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(SmallintType.SMALLINT, 100L, true, 1000L, true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(SmallintType.SMALLINT, 100L))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addSmallintValues(PARTITION_KEY, 1L, 10000L)
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(SmallintType.SMALLINT, 1000L))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_SMALLINT,
                PARTITION_KEY,
                ImmutableList.of("1", "100", "1000", "10000"),
                ImmutableList.of(singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
                        ImmutableList.of("1000"),
                        ImmutableList.of("1000", "10000"),
                        ImmutableList.of("100", "1000"),
                        ImmutableList.of("100", "1000", "10000"),
                        ImmutableList.of("1", "10000"),
                        ImmutableList.of("1", "100"),
                        ImmutableList.of("1", "100", "1000", "10000")));
    }

    @Test
    public void testGetPartitionsFilterTinyInt()
            throws Exception
    {
        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addTinyintValues(PARTITION_KEY, 127L)
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(TinyintType.TINYINT, 10L))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(TinyintType.TINYINT, 10L, true, 100L, true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(TinyintType.TINYINT, 10L))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addTinyintValues(PARTITION_KEY, 1L, 127L)
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(TinyintType.TINYINT, 100L))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_TINYINT,
                PARTITION_KEY,
                ImmutableList.of("1", "10", "100", "127"),
                ImmutableList.of(singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
                        ImmutableList.of("127"),
                        ImmutableList.of("100", "127"),
                        ImmutableList.of("10", "100"),
                        ImmutableList.of("10", "100", "127"),
                        ImmutableList.of("1", "127"),
                        ImmutableList.of("1", "10"),
                        ImmutableList.of("1", "10", "100", "127")));
    }

    @Test
    public void testGetPartitionsFilterTinyIntNegatives()
            throws Exception
    {
        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addTinyintValues(PARTITION_KEY, -128L)
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(TinyintType.TINYINT, 0L))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(TinyintType.TINYINT, 0L, true, 50L, true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(TinyintType.TINYINT, 0L))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addTinyintValues(PARTITION_KEY, 0L, -128L)
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(TinyintType.TINYINT, 0L))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_TINYINT,
                PARTITION_KEY,
                ImmutableList.of("-128", "0", "50", "100"),
                ImmutableList.of(singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
                        ImmutableList.of("-128"),
                        ImmutableList.of("100", "50"),
                        ImmutableList.of("0", "50"),
                        ImmutableList.of("0", "100", "50"),
                        ImmutableList.of("-128", "0"),
                        ImmutableList.of("-128"),
                        ImmutableList.of("-128", "0", "100", "50")));
    }

    @Test
    public void testGetPartitionsFilterDecimal()
            throws Exception
    {
        String value1 = "1.000";
        String value2 = "10.134";
        String value3 = "25.111";
        String value4 = "30.333";

        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDecimalValues(PARTITION_KEY, value1)
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(DECIMAL_TYPE, decimalOf(value2)))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(DECIMAL_TYPE, decimalOf(value2), true, decimalOf(value3), true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(DECIMAL_TYPE, decimalOf(value3)))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDecimalValues(PARTITION_KEY, value1, value4)
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(DECIMAL_TYPE, decimalOf("25.5")))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_DECIMAL,
                PARTITION_KEY,
                ImmutableList.of(value1, value2, value3, value4),
                ImmutableList.of(singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
                        ImmutableList.of(value1),
                        ImmutableList.of(value3, value4),
                        ImmutableList.of(value2, value3),
                        ImmutableList.of(value3, value4),
                        ImmutableList.of(value1, value4),
                        ImmutableList.of(value1, value2, value3),
                        ImmutableList.of(value1, value2, value3, value4)));
    }

    // we don't presently know how to properly convert a Date type into a string that is compatible with Glue.
    @Test
    public void testGetPartitionsFilterDate()
            throws Exception
    {
        Map<Column, Domain> singleEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDateValues(PARTITION_KEY, 18000L)
                .build();
        Map<Column, Domain> greaterThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThan(DateType.DATE, 19000L))
                .build();
        Map<Column, Domain> betweenInclusive = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.range(DateType.DATE, 19000L, true, 20000L, true))
                .build();
        Map<Column, Domain> greaterThanOrEquals = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(DateType.DATE, 19000L))
                .build();
        Map<Column, Domain> inClause = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDateValues(PARTITION_KEY, 18000L, 21000L)
                .build();
        Map<Column, Domain> lessThan = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.lessThan(DateType.DATE, 20000L))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();
        // we are unable to convert Date to a string format that Glue will accept, so it should translate to the wildcard in all cases. Commented out results are
        // what we expect if we are able to do a proper conversion
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_DATE,
                PARTITION_KEY,
                ImmutableList.of("18000", "19000", "20000", "21000"),
                ImmutableList.of(
                        singleEquals, greaterThan, betweenInclusive, greaterThanOrEquals, inClause, lessThan, all),
                ImmutableList.of(
//                        ImmutableList.of("18000"),
//                        ImmutableList.of("20000", "21000"),
//                        ImmutableList.of("19000", "20000"),
//                        ImmutableList.of("19000", "20000", "21000"),
//                        ImmutableList.of("18000", "21000"),
//                        ImmutableList.of("18000", "19000"),
                        ImmutableList.of("18000", "19000", "20000", "21000"),
                        ImmutableList.of("18000", "19000", "20000", "21000"),
                        ImmutableList.of("18000", "19000", "20000", "21000"),
                        ImmutableList.of("18000", "19000", "20000", "21000"),
                        ImmutableList.of("18000", "19000", "20000", "21000"),
                        ImmutableList.of("18000", "19000", "20000", "21000"),
                        ImmutableList.of("18000", "19000", "20000", "21000")));
    }

    @Test
    public void testGetPartitionsFilterTwoPartitionKeys()
            throws Exception
    {
        Map<Column, Domain> equalsFilter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues(PARTITION_KEY, "2020-03-01")
                .addBigintValues(PARTITION_KEY2, 300L)
                .build();
        Map<Column, Domain> rangeFilter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addRanges(PARTITION_KEY, Range.greaterThanOrEqual(VarcharType.VARCHAR, utf8Slice("2020-02-01")))
                .addRanges(PARTITION_KEY2, Range.greaterThan(BIGINT, 200L))
                .build();
        Map<Column, Domain> all = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.all(VARCHAR))
                .build();

        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_TWO_KEYS,
                ImmutableList.of(PARTITION_KEY, PARTITION_KEY2),
                ImmutableList.of(
                        PartitionValues.make("2020-01-01", "100"),
                        PartitionValues.make("2020-02-01", "200"),
                        PartitionValues.make("2020-03-01", "300"),
                        PartitionValues.make("2020-04-01", "400")),
                ImmutableList.of(equalsFilter, rangeFilter, all),
                ImmutableList.of(
                        ImmutableList.of(PartitionValues.make("2020-03-01", "300")),
                        ImmutableList.of(
                                PartitionValues.make("2020-03-01", "300"),
                                PartitionValues.make("2020-04-01", "400")),
                        ImmutableList.of(
                                PartitionValues.make("2020-01-01", "100"),
                                PartitionValues.make("2020-02-01", "200"),
                                PartitionValues.make("2020-03-01", "300"),
                                PartitionValues.make("2020-04-01", "400"))));
    }

    @Test
    public void testGetPartitionsFilterMaxLengthWildcard()
            throws Exception
    {
        // this filter string will exceed the 2048 char limit set by glue, and we expect the filter to revert to the wildcard
        Map<Column, Domain> filter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues(PARTITION_KEY, Strings.repeat("x", 2048))
                .build();

        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_VARCHAR,
                PARTITION_KEY,
                VARCHAR_PARTITION_VALUES,
                ImmutableList.of(filter),
                ImmutableList.of(
                        ImmutableList.of("2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01")));
    }

    @Test
    public void testGetPartitionsFilterTwoPartitionKeysPartialQuery()
            throws Exception
    {
        // we expect the second constraint to still be present and provide filtering
        Map<Column, Domain> equalsFilter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues(PARTITION_KEY, Strings.repeat("x", 2048))
                .addBigintValues(PARTITION_KEY2, 300L)
                .build();

        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_TWO_KEYS,
                ImmutableList.of(PARTITION_KEY, PARTITION_KEY2),
                ImmutableList.of(
                        PartitionValues.make("2020-01-01", "100"),
                        PartitionValues.make("2020-02-01", "200"),
                        PartitionValues.make("2020-03-01", "300"),
                        PartitionValues.make("2020-04-01", "400")),
                ImmutableList.of(equalsFilter),
                ImmutableList.of(ImmutableList.of(PartitionValues.make("2020-03-01", "300"))));
    }

    @Test
    public void testGetPartitionsFilterNone()
            throws Exception
    {
        // test both a single column none, and a valid domain with none()
        Map<Column, Domain> noneFilter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.none(VarcharType.VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_VARCHAR,
                PARTITION_KEY,
                VARCHAR_PARTITION_VALUES,
                ImmutableList.of(noneFilter),
                ImmutableList.of(ImmutableList.of("2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01")));
    }

    @Test
    public void testGetPartitionsFilterNotNull()
            throws Exception
    {
        Map<Column, Domain> notNullFilter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.notNull(VarcharType.VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_VARCHAR,
                PARTITION_KEY,
                VARCHAR_PARTITION_VALUES,
                ImmutableList.of(notNullFilter),
                ImmutableList.of(ImmutableList.of("2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01")));
    }

    @Test
    public void testGetPartitionsFilterIsNull()
            throws Exception
    {
        Map<Column, Domain> isNullFilter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.onlyNull(VarcharType.VARCHAR))
                .build();
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_VARCHAR,
                PARTITION_KEY,
                VARCHAR_PARTITION_VALUES,
                ImmutableList.of(isNullFilter),
                ImmutableList.of(ImmutableList.of()));
    }

    @Test
    public void testGetPartitionsFilterIsNullWithValue()
            throws Exception
    {
        Map<Column, Domain> isNullFilter = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain(PARTITION_KEY, Domain.onlyNull(VarcharType.VARCHAR))
                .build();
        List<String> partitionList = new ArrayList<>();
        partitionList.add(null);
        doGetPartitionsFilterTest(
                CREATE_TABLE_COLUMNS_PARTITIONED_VARCHAR,
                PARTITION_KEY,
                partitionList,
                ImmutableList.of(isNullFilter),
                ImmutableList.of(ImmutableList.of(HIVE_DEFAULT_DYNAMIC_PARTITION)));
    }

    private void doGetPartitionsFilterTest(
            List<ColumnMetadata> columnMetadata,
            String partitionColumnName,
            List<String> partitionStringValues,
            List<Map<Column, Domain>> filterList,
            List<List<String>> expectedSingleValueList)
            throws Exception
    {
        List<PartitionValues> partitionValuesList = partitionStringValues.stream()
                .map(PartitionValues::make)
                .collect(toImmutableList());
        List<List<PartitionValues>> expectedPartitionValuesList = expectedSingleValueList.stream()
                .map(expectedValue -> expectedValue.stream()
                        .map(PartitionValues::make)
                        .collect(toImmutableList()))
                .collect(toImmutableList());
        doGetPartitionsFilterTest(columnMetadata, ImmutableList.of(partitionColumnName), partitionValuesList, filterList, expectedPartitionValuesList);
    }

    /**
     * @param filterList should be same sized list as expectedValuesList
     * @param expectedValuesList
     * @throws Exception
     */
    private void doGetPartitionsFilterTest(
            List<ColumnMetadata> columnMetadata,
            List<String> partitionColumnNames,
            List<PartitionValues> partitionValues,
            List<Map<Column, Domain>> filterList,
            List<List<PartitionValues>> expectedValuesList)
            throws Exception
    {
        try (CloseableSchemaTableName closeableTableName = new CloseableSchemaTableName(temporaryTable(("get_partitions")))) {
            SchemaTableName tableName = closeableTableName.getSchemaTableName();
            createDummyPartitionedTable(tableName, columnMetadata, partitionColumnNames, partitionValues);
            ExtendedHiveMetastore metastoreClient = getMetastoreClient();

            for (int i = 0; i < filterList.size(); i++) {
                Map<Column, Domain> filter = filterList.get(i);
                List<PartitionValues> expectedValues = expectedValuesList.get(i);
                List<String> expectedResults = expectedValues.stream()
                        .map(expectedPartitionValues -> makePartName(partitionColumnNames, expectedPartitionValues.getValues()))
                        .collect(toImmutableList());

                List<PartitionNameWithVersion> partitionNames = metastoreClient.getPartitionNamesByFilter(
                        METASTORE_CONTEXT,
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        filter);
                assertEquals(
                        partitionNames,
                        expectedResults,
                        format("lists \nactual: %s\nexpected: %s\nmismatch for filter %s (input index %d)\n", partitionNames, expectedResults, filter, i));
            }
        }
    }

    private void createDummyPartitionedTable(SchemaTableName tableName, List<ColumnMetadata> columns, List<String> partitionColumnNames, List<PartitionValues> partitionValues)
            throws Exception
    {
        doCreateEmptyTable(tableName, ORC, columns, partitionColumnNames);

        ExtendedHiveMetastore metastoreClient = getMetastoreClient();
        Table table = metastoreClient.getTable(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        List<String> partitionNames = new ArrayList<>();
        partitionValues.stream()
                .map(partitionValue -> makePartName(partitionColumnNames, partitionValue.values))
                .forEach(
                        partitionName -> {
                            partitions.add(new PartitionWithStatistics(createDummyPartition(table, partitionName), partitionName, PartitionStatistics.empty()));
                            partitionNames.add(partitionName);
                        });
        metastoreClient.addPartitions(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), partitions);
        partitionNames.forEach(
                partitionName -> metastoreClient.updatePartitionStatistics(
                        METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), partitionName, currentStatistics -> EMPTY_TABLE_STATISTICS));
    }

    private class CloseableSchemaTableName
            implements AutoCloseable
    {
        private final SchemaTableName schemaTableName;

        private CloseableSchemaTableName(SchemaTableName schemaTableName)
        {
            this.schemaTableName = schemaTableName;
        }

        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public void close()
        {
            dropTable(schemaTableName);
        }
    }

    // container class for readability. Each value is one for a partitionKey, in order they appear in the schema
    private static class PartitionValues
    {
        private final List<String> values;

        private static PartitionValues make(String... values)
        {
            return new PartitionValues(Arrays.asList(values));
        }

        private PartitionValues(List<String> values)
        {
            this.values = values;
        }

        public List<String> getValues()
        {
            return values;
        }
    }
}
