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

import com.facebook.presto.hadoop.HadoopFileStatus;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.orc.DwrfHiveRecordCursor;
import com.facebook.presto.hive.orc.DwrfRecordCursorProvider;
import com.facebook.presto.hive.orc.OrcHiveRecordCursor;
import com.facebook.presto.hive.orc.OrcPageSource;
import com.facebook.presto.hive.orc.OrcRecordCursorProvider;
import com.facebook.presto.hive.parquet.ParquetHiveRecordCursor;
import com.facebook.presto.hive.rcfile.RcFilePageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveMetadata.convertToPredicate;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.SEQUENCEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.hive.TestingFixture.BUCKETED_BY_BIGINT_BOOLEAN;
import static com.facebook.presto.hive.TestingFixture.BUCKETED_BY_DOUBLE_FLOAT;
import static com.facebook.presto.hive.TestingFixture.BUCKETED_BY_STRING_INT;
import static com.facebook.presto.hive.TestingFixture.CREATE;
import static com.facebook.presto.hive.TestingFixture.CREATE_EMPTY;
import static com.facebook.presto.hive.TestingFixture.CREATE_SAMPLED;
import static com.facebook.presto.hive.TestingFixture.CREATE_TABLE_COLUMNS;
import static com.facebook.presto.hive.TestingFixture.CREATE_TABLE_COLUMNS_PARTITIONED;
import static com.facebook.presto.hive.TestingFixture.CREATE_TABLE_DATA;
import static com.facebook.presto.hive.TestingFixture.CREATE_TABLE_PARTITIONED_DATA;
import static com.facebook.presto.hive.TestingFixture.CREATE_TABLE_PARTITIONED_DATA_2ND;
import static com.facebook.presto.hive.TestingFixture.CREATE_VIEW;
import static com.facebook.presto.hive.TestingFixture.INSERT;
import static com.facebook.presto.hive.TestingFixture.INSERT_INTO_EXISTING_PARTITION_TABLE;
import static com.facebook.presto.hive.TestingFixture.INSERT_INTO_NEW_PARTITION_TABLE;
import static com.facebook.presto.hive.TestingFixture.INVALID_COLUMN;
import static com.facebook.presto.hive.TestingFixture.INVALID_DATABASE;
import static com.facebook.presto.hive.TestingFixture.INVALID_TABLE;
import static com.facebook.presto.hive.TestingFixture.METADATA_DELETE;
import static com.facebook.presto.hive.TestingFixture.OFFLINE_PARTITION;
import static com.facebook.presto.hive.TestingFixture.OFFLINE_TABLE;
import static com.facebook.presto.hive.TestingFixture.PARTITION_SCHEMA_CHANGE;
import static com.facebook.presto.hive.TestingFixture.PARTITION_SCHEMA_CHANGE_NON_CANONICAL;
import static com.facebook.presto.hive.TestingFixture.RENAME_NEW;
import static com.facebook.presto.hive.TestingFixture.RENAME_OLD;
import static com.facebook.presto.hive.TestingFixture.UNPARTITIONED_TABLE;
import static com.facebook.presto.hive.TestingFixture.VIEW;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "hive")
public abstract class AbstractTestHiveClient
{
    private final TestingFixture fixture;
    private final TestingEnvironment runtime;

    public AbstractTestHiveClient(TestingEnvironment runtime, TestingFixture fixture)
    {
        this.fixture = fixture;
        this.runtime = runtime;
    }

    @AfterClass
    public void close()
    {
        runtime.close();
    }

    protected ConnectorSession newSession()
    {
        return new TestingConnectorSession(new HiveSessionProperties(new HiveClientConfig()).getSessionProperties());
    }

    private SchemaTableName getTable(String tableName)
    {
        return new SchemaTableName(fixture.getDatabaseName(), tableName);
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = runtime.getMetadata().listSchemaNames(newSession());
        assertTrue(databases.contains(fixture.getDatabaseName()));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = runtime.getMetadata().listTables(newSession(), fixture.getDatabaseName());
        assertTrue(tables.contains(fixture.getTablePartitionFormat()));
        assertTrue(tables.contains(getTable(UNPARTITIONED_TABLE)));
    }

    @Test
    public void testGetAllTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = runtime.getMetadata().listTables(newSession(), null);
        assertTrue(tables.contains(fixture.getTablePartitionFormat()));
        assertTrue(tables.contains(getTable(UNPARTITIONED_TABLE)));
    }

    @Test
    public void testGetAllTableColumns()
    {
        Map<SchemaTableName, List<ColumnMetadata>> allColumns = runtime.getMetadata().listTableColumns(newSession(), new SchemaTablePrefix());
        assertTrue(allColumns.containsKey(fixture.getTablePartitionFormat()));
        assertTrue(allColumns.containsKey(getTable(UNPARTITIONED_TABLE)));
    }

    @Test
    public void testGetAllTableColumnsInSchema()
    {
        Map<SchemaTableName, List<ColumnMetadata>> allColumns = runtime.getMetadata().listTableColumns(newSession(), new SchemaTablePrefix(fixture.getDatabaseName()));
        assertTrue(allColumns.containsKey(fixture.getTablePartitionFormat()));
        assertTrue(allColumns.containsKey(getTable(UNPARTITIONED_TABLE)));
    }

    @Test
    public void testListUnknownSchema()
    {
        ConnectorSession session = newSession();
        assertNull(runtime.getMetadata().getTableHandle(session, new SchemaTableName(INVALID_DATABASE, INVALID_TABLE)));
        assertEquals(runtime.getMetadata().listTables(session, INVALID_DATABASE), ImmutableList.of());
        assertEquals(runtime.getMetadata().listTableColumns(session, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
        assertEquals(runtime.getMetadata().listViews(session, INVALID_DATABASE), ImmutableList.of());
        assertEquals(runtime.getMetadata().getViews(session, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(fixture.getTablePartitionFormat());
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), fixture.getTableLayout());
    }

    @Test
    public void testGetPartitionsWithBindings()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(fixture.getTablePartitionFormat());
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.withColumnDomains(ImmutableMap.of(fixture.getIntColumn(), Domain.singleValue(BIGINT, 5L))), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), fixture.getTableLayout());
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        runtime.getMetadata().getTableLayouts(newSession(), fixture.getInvalidTableHandle(), new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(fixture.getTablePartitionFormat());
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), fixture.getTableLayout());
    }

    protected void assertExpectedTableLayout(ConnectorTableLayout actualTableLayout, ConnectorTableLayout expectedTableLayout)
    {
        assertExpectedTableLayoutHandle(actualTableLayout.getHandle(), expectedTableLayout.getHandle());
        assertEquals(actualTableLayout.getPredicate(), expectedTableLayout.getPredicate());
        assertEquals(actualTableLayout.getDiscretePredicates().isPresent(), expectedTableLayout.getDiscretePredicates().isPresent());
        actualTableLayout.getDiscretePredicates().ifPresent(actual -> assertEqualsIgnoreOrder(actual, expectedTableLayout.getDiscretePredicates().get()));
        assertEquals(actualTableLayout.getPartitioningColumns(), expectedTableLayout.getPartitioningColumns());
        assertEquals(actualTableLayout.getLocalProperties(), expectedTableLayout.getLocalProperties());
    }

    protected void assertExpectedTableLayoutHandle(ConnectorTableLayoutHandle actualTableLayoutHandle, ConnectorTableLayoutHandle expectedTableLayoutHandle)
    {
        assertInstanceOf(actualTableLayoutHandle, HiveTableLayoutHandle.class);
        assertInstanceOf(expectedTableLayoutHandle, HiveTableLayoutHandle.class);
        HiveTableLayoutHandle actual = (HiveTableLayoutHandle) actualTableLayoutHandle;
        HiveTableLayoutHandle expected = (HiveTableLayoutHandle) expectedTableLayoutHandle;
        assertEquals(actual.getClientId(), expected.getClientId());
        assertExpectedPartitions(actual.getPartitions().get(), expected.getPartitions().get());
    }

    protected void assertExpectedPartitions(List<?> actualPartitions, Iterable<?> expectedPartitions)
    {
        Map<String, ?> actualById = uniqueIndex(actualPartitions, actualPartition -> ((HivePartition) actualPartition).getPartitionId());
        for (Object expected : expectedPartitions) {
            assertInstanceOf(expected, HivePartition.class);
            HivePartition expectedPartition = (HivePartition) expected;

            Object actual = actualById.get(expectedPartition.getPartitionId());
            assertEquals(actual, expected);
            assertInstanceOf(actual, HivePartition.class);
            HivePartition actualPartition = (HivePartition) actual;

            assertNotNull(actualPartition, "partition " + expectedPartition.getPartitionId());
            assertEquals(actualPartition.getPartitionId(), expectedPartition.getPartitionId());
            assertEquals(actualPartition.getKeys(), expectedPartition.getKeys());
            assertEquals(actualPartition.getTableName(), expectedPartition.getTableName());
            assertEquals(actualPartition.getBucket(), expectedPartition.getBucket());
            assertEquals(actualPartition.getTupleDomain(), expectedPartition.getTupleDomain());
        }
    }

    @Test
    public void testGetPartitionNamesUnpartitioned()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(getTable(UNPARTITIONED_TABLE));
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertEquals(getAllPartitions(getOnlyElement(tableLayoutResults).getTableLayout().getHandle()).size(), 1);
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), fixture.getUnpartitionedTableLayout());
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        runtime.getMetadata().getTableLayouts(newSession(), fixture.getInvalidTableHandle(), new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
    }

    @SuppressWarnings({"ValueOfIncrementOrDecrementUsed", "UnusedAssignment"})
    @Test
    public void testGetTableSchemaPartitionFormat()
            throws Exception
    {
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(newSession(), getTableHandle(fixture.getTablePartitionFormat()));
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
        assertPrimitiveField(map, "t_tinyint", BIGINT, false);
        assertPrimitiveField(map, "t_smallint", BIGINT, false);
        assertPrimitiveField(map, "t_int", BIGINT, false);
        assertPrimitiveField(map, "t_bigint", BIGINT, false);
        assertPrimitiveField(map, "t_float", DOUBLE, false);
        assertPrimitiveField(map, "t_double", DOUBLE, false);
        assertPrimitiveField(map, "t_boolean", BOOLEAN, false);
        assertPrimitiveField(map, "ds", VARCHAR, true);
        assertPrimitiveField(map, "file_format", VARCHAR, true);
        assertPrimitiveField(map, "dummy", BIGINT, true);
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(getTable(UNPARTITIONED_TABLE));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(newSession(), tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
        assertPrimitiveField(map, "t_tinyint", BIGINT, false);
    }

    @Test
    public void testGetTableSchemaOffline()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(getTable(OFFLINE_TABLE));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(newSession(), tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(getTable(OFFLINE_PARTITION));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(newSession(), tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
    }

    @Test
    public void testGetTableSchemaException()
            throws Exception
    {
        assertNull(runtime.getMetadata().getTableHandle(newSession(), getTable(INVALID_TABLE)));
    }

    @Test
    public void testGetPartitionSplitsBatch()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(fixture.getTablePartitionFormat());
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorSplitSource splitSource = runtime.getSplitManager().getSplits(session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

        assertEquals(getSplitCount(splitSource), fixture.getPartitionCount());
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(getTable(UNPARTITIONED_TABLE));
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorSplitSource splitSource = runtime.getSplitManager().getSplits(session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

        assertEquals(getSplitCount(splitSource), 1);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
            throws Exception
    {
        runtime.getSplitManager().getSplits(newSession(), fixture.getInvalidTableLayoutHandle());
    }

    @Test
    public void testGetPartitionSplitsEmpty()
            throws Exception
    {
        ConnectorSplitSource splitSource = runtime.getSplitManager().getSplits(newSession(), fixture.getEmptyTableLayoutHandle());
        // fetch full list
        getSplitCount(splitSource);
    }

    @Test
    public void testGetPartitionTableOffline()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(getTable(OFFLINE_TABLE));
        try {
            runtime.getMetadata().getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            fail("expected TableOfflineException");
        }
        catch (TableOfflineException e) {
            assertEquals(e.getTableName(), getTable(OFFLINE_TABLE));
        }
    }

    @Test
    public void testGetPartitionSplitsTableOfflinePartition()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(getTable(OFFLINE_PARTITION));
        assertNotNull(tableHandle);

        ColumnHandle dsColumn = runtime.getMetadata().getColumnHandles(session, tableHandle).get("ds");
        assertNotNull(dsColumn);

        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("2012-12-30"));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(dsColumn, domain));
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        try {
            getSplitCount(runtime.getSplitManager().getSplits(session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
            fail("Expected PartitionOfflineException");
        }
        catch (PartitionOfflineException e) {
            assertEquals(e.getTableName(), getTable(OFFLINE_PARTITION));
            assertEquals(e.getPartition(), "ds=2012-12-30");
        }
    }

    @Test
    public void testBucketedTableStringInt()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(getTable(BUCKETED_BY_STRING_INT));
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "test";
        Long testInt = 13L;
        Long testSmallint = 12L;

        // Reverse the order of bindings as compared to bucketing order
        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandles.get(columnIndex.get("t_int")), NullableValue.of(BIGINT, testInt))
                .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(VARCHAR, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_smallint")), NullableValue.of(BIGINT, testSmallint))
                .build();

        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

        boolean rowFound = false;
        for (MaterializedRow row : result) {
            if (testString.equals(row.getField(columnIndex.get("t_string"))) &&
                    testInt.equals(row.getField(columnIndex.get("t_int"))) &&
                    testSmallint.equals(row.getField(columnIndex.get("t_smallint")))) {
                rowFound = true;
            }
        }
        assertTrue(rowFound);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testBucketedTableBigintBoolean()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(getTable(BUCKETED_BY_BIGINT_BOOLEAN));
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "test";
        Long testBigint = 89L;
        Boolean testBoolean = true;

        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(VARCHAR, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_bigint")), NullableValue.of(BIGINT, testBigint))
                .put(columnHandles.get(columnIndex.get("t_boolean")), NullableValue.of(BOOLEAN, testBoolean))
                .build();

        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

        boolean rowFound = false;
        for (MaterializedRow row : result) {
            if (testString.equals(row.getField(columnIndex.get("t_string"))) &&
                    testBigint.equals(row.getField(columnIndex.get("t_bigint"))) &&
                    testBoolean.equals(row.getField(columnIndex.get("t_boolean")))) {
                rowFound = true;
                break;
            }
        }
        assertTrue(rowFound);
    }

    @Test
    public void testBucketedTableDoubleFloat()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(getTable(BUCKETED_BY_DOUBLE_FLOAT));
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandles.get(columnIndex.get("t_float")), NullableValue.of(DOUBLE, 87.1))
                .put(columnHandles.get(columnIndex.get("t_double")), NullableValue.of(DOUBLE, 88.2))
                .build();

        // floats and doubles are not supported, so we should see all splits
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(32), Optional.empty());
        assertEquals(result.getRowCount(), 100);
    }

    private void assertTableIsBucketed(ConnectorTableHandle tableHandle)
            throws Exception
    {
        // the bucketed test tables should have exactly 32 splits
        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 32);

        // verify all paths are unique
        Set<String> paths = new HashSet<>();
        for (ConnectorSplit split : splits) {
            assertTrue(paths.add(((HiveSplit) split).getPath()));
        }
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(fixture.getTablePartitionFormat());
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(session, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), fixture.getPartitionCount());
        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileFormat = partitionKeys.get(1).getValue();
            HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase());
            long dummyPartition = Long.parseLong(partitionKeys.get(2).getValue());

            long rowNumber = 0;
            long completedBytes = 0;
            try (ConnectorPageSource pageSource = runtime.getPageSourceProvider().createPageSource(session, hiveSplit, columnHandles)) {
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));

                assertPageSourceType(pageSource, fileType);

                for (MaterializedRow row : result) {
                    try {
                        assertValueTypes(row, tableMetadata.getColumns());
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertNull(row.getField(columnIndex.get("t_string")));
                    }
                    else if (rowNumber % 19 == 1) {
                        assertEquals(row.getField(columnIndex.get("t_string")), "");
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_string")), "test");
                    }

                    assertEquals(row.getField(columnIndex.get("t_tinyint")), 1 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("t_smallint")), 2 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("t_int")), 3 + rowNumber);

                    if (rowNumber % 13 == 0) {
                        assertNull(row.getField(columnIndex.get("t_bigint")));
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_bigint")), 4 + rowNumber);
                    }

                    assertEquals((Double) row.getField(columnIndex.get("t_float")), 5.1 + rowNumber, 0.001);
                    assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);

                    if (rowNumber % 3 == 2) {
                        assertNull(row.getField(columnIndex.get("t_boolean")));
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_boolean")), rowNumber % 3 != 0);
                    }

                    assertEquals(row.getField(columnIndex.get("ds")), ds);
                    assertEquals(row.getField(columnIndex.get("file_format")), fileFormat);
                    assertEquals(row.getField(columnIndex.get("dummy")), dummyPartition);

                    long newCompletedBytes = pageSource.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    assertTrue(newCompletedBytes <= hiveSplit.getLength());
                    completedBytes = newCompletedBytes;
                }

                assertTrue(completedBytes <= hiveSplit.getLength());
                assertEquals(rowNumber, 100);
            }
        }
    }

    @Test
    public void testGetPartialRecords()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(fixture.getTablePartitionFormat());
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), fixture.getPartitionCount());
        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileFormat = partitionKeys.get(1).getValue();
            HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase());
            long dummyPartition = Long.parseLong(partitionKeys.get(2).getValue());

            long rowNumber = 0;
            try (ConnectorPageSource pageSource = runtime.getPageSourceProvider().createPageSource(session, hiveSplit, columnHandles)) {
                assertPageSourceType(pageSource, fileType);
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                for (MaterializedRow row : result) {
                    rowNumber++;

                    assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("ds")), ds);
                    assertEquals(row.getField(columnIndex.get("file_format")), fileFormat);
                    assertEquals(row.getField(columnIndex.get("dummy")), dummyPartition);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(getTable(UNPARTITIONED_TABLE));
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 1);

        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            try (ConnectorPageSource pageSource = runtime.getPageSourceProvider().createPageSource(session, split, columnHandles)) {
                assertPageSourceType(pageSource, TEXTFILE);
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));

                assertEquals(pageSource.getTotalBytes(), hiveSplit.getLength());
                for (MaterializedRow row : result) {
                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertNull(row.getField(columnIndex.get("t_string")));
                    }
                    else if (rowNumber % 19 == 1) {
                        assertEquals(row.getField(columnIndex.get("t_string")), "");
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_string")), "unpartitioned");
                    }

                    assertEquals(row.getField(columnIndex.get("t_tinyint")), 1 + rowNumber);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetRecordsInvalidColumn()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(getTable(UNPARTITIONED_TABLE));
        readTable(table, ImmutableList.of(fixture.getInvalidColumnHandle()), newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*The column 't_data' in table '.*\\.presto_test_partition_schema_change' is declared as type 'bigint', but partition 'ds=2012-12-29' declared column 't_data' as type 'string'.")
    public void testPartitionSchemaMismatch()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(getTable(PARTITION_SCHEMA_CHANGE));
        readTable(table, ImmutableList.of(fixture.getDsColumn()), newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
    }

    @Test
    public void testPartitionSchemaNonCanonical()
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle table = getTableHandle(getTable(PARTITION_SCHEMA_CHANGE_NON_CANONICAL));
        ColumnHandle column = runtime.getMetadata().getColumnHandles(session, table).get("t_boolean");
        assertNotNull(column);
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, table, new Constraint<>(TupleDomain.fromFixedValues(ImmutableMap.of(column, NullableValue.of(BOOLEAN, false))), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        assertEquals(getPartitionId(getAllPartitions(layoutHandle).get(0)), "t_boolean=0");

        ConnectorSplitSource splitSource = runtime.getSplitManager().getSplits(session, layoutHandle);
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        ImmutableList<ColumnHandle> columnHandles = ImmutableList.of(column);
        try (ConnectorPageSource ignored = runtime.getPageSourceProvider().createPageSource(session, split, columnHandles)) {
            // TODO coercion of non-canonical values should be supported
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HIVE_INVALID_PARTITION_VALUE.toErrorCode());
        }
    }

    @Test
    public void testTypesTextFile()
            throws Exception
    {
        assertGetRecords("presto_test_types_textfile", TEXTFILE);
    }

    @Test
    public void testTypesSequenceFile()
            throws Exception
    {
        assertGetRecords("presto_test_types_sequencefile", SEQUENCEFILE);
    }

    @Test
    public void testTypesRcText()
            throws Exception
    {
        assertGetRecords("presto_test_types_rctext", RCTEXT);
    }

    @Test
    public void testTypesRcTextRecordCursor()
            throws Exception
    {
        ConnectorSession session = newSession();

        if (runtime.getMetadata().getTableHandle(session, new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_rctext")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_rctext"));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(runtime.getTimeZone().getID()),
                runtime.getHdfsEnvironment(),
                ImmutableSet.<HiveRecordCursorProvider>of(new ColumnarTextHiveRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(session, hiveSplit, columnHandles);
        assertGetRecords(RCTEXT, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testTypesRcBinary()
            throws Exception
    {
        assertGetRecords("presto_test_types_rcbinary", RCBINARY);
    }

    @Test
    public void testTypesRcBinaryRecordCursor()
            throws Exception
    {
        ConnectorSession session = newSession();

        if (runtime.getMetadata().getTableHandle(session, new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_rcbinary")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_rcbinary"));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(runtime.getTimeZone().getID()),
                runtime.getHdfsEnvironment(),
                ImmutableSet.<HiveRecordCursorProvider>of(new ColumnarBinaryHiveRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(session, hiveSplit, columnHandles);
        assertGetRecords(RCBINARY, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testTypesOrc()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_orc", ORC);
    }

    @Test
    public void testTypesOrcRecordCursor()
            throws Exception
    {
        ConnectorSession session = newSession();

        if (runtime.getMetadata().getTableHandle(session, new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_orc")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_orc"));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(runtime.getTimeZone().getID()),
                runtime.getHdfsEnvironment(),
                ImmutableSet.<HiveRecordCursorProvider>of(new OrcRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(session, hiveSplit, columnHandles);
        assertGetRecords(ORC, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testTypesParquet()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_parquet", PARQUET);
    }

    @Test
    public void testTypesDwrf()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_dwrf", DWRF);
    }

    @Test
    public void testTypesDwrfRecordCursor()
            throws Exception
    {
        ConnectorSession session = newSession();

        if (runtime.getMetadata().getTableHandle(session, new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_dwrf")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(fixture.getDatabaseName(), "presto_test_types_dwrf"));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        ReaderWriterProfiler.setProfilerOptions(new Configuration());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(runtime.getTimeZone().getID()),
                runtime.getHdfsEnvironment(),
                ImmutableSet.<HiveRecordCursorProvider>of(new DwrfRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(session, hiveSplit, columnHandles);
        assertGetRecords(DWRF, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testHiveViewsAreNotSupported()
            throws Exception
    {
        try {
            getTableHandle(getTable(VIEW));
            fail("Expected HiveViewNotSupportedException");
        }
        catch (HiveViewNotSupportedException e) {
            assertEquals(e.getTableName(), getTable(VIEW));
        }
    }

    @Test
    public void testHiveViewsHaveNoColumns()
            throws Exception
    {
        assertEquals(runtime.getMetadata().listTableColumns(newSession(), new SchemaTablePrefix(getTable(VIEW).getSchemaName(), getTable(VIEW).getTableName())), ImmutableMap.of());
    }

    @Test
    public void testRenameTable()
    {
        try {
            createDummyTable(getTable(RENAME_OLD));
            ConnectorSession session = newSession();

            runtime.getMetadata().renameTable(session, getTableHandle(getTable(RENAME_OLD)), getTable(RENAME_NEW));

            assertNull(runtime.getMetadata().getTableHandle(session, getTable(RENAME_OLD)));
            assertNotNull(runtime.getMetadata().getTableHandle(session, getTable(RENAME_NEW)));
        }
        finally {
            dropTable(getTable(RENAME_OLD));
            dropTable(getTable(RENAME_NEW));
        }
    }

    @Test
    public void testTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : fixture.getCreateTableFormats()) {
            try {
                doCreateTable(getTable(CREATE), storageFormat);
            }
            finally {
                dropTable(getTable(CREATE));
            }
        }
    }

    @Test
    public void testTableCreationRollback()
            throws Exception
    {
        try {
            ConnectorSession session = newSession();

            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(getTable(CREATE), CREATE_TABLE_COLUMNS, createTableProperties(RCBINARY), session.getUser());

            ConnectorOutputTableHandle outputHandle = runtime.getMetadata().beginCreateTable(session, tableMetadata);

            // write the data
            ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, outputHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
            sink.commit();

            // verify we have data files
            assertFalse(listAllDataFiles(outputHandle).isEmpty());

            // rollback the table
            runtime.getMetadata().rollbackCreateTable(session, outputHandle);

            // verify all files have been deleted
            assertTrue(listAllDataFiles(outputHandle).isEmpty());

            // verify table is not in the metastore
            assertNull(runtime.getMetadata().getTableHandle(session, getTable(CREATE)));
        }
        finally {
            dropTable(getTable(CREATE));
        }
    }

    @Test
    public void testInsert()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : fixture.getCreateTableFormats()) {
            try {
                doInsert(storageFormat, getTable(INSERT));
            }
            finally {
                dropTable(getTable(INSERT));
            }
        }
    }

    @Test
    public void testInsertIntoNewPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : fixture.getCreateTableFormats()) {
            try {
                doInsertIntoNewPartition(storageFormat, getTable(INSERT_INTO_NEW_PARTITION_TABLE));
            }
            finally {
                dropTable(getTable(INSERT_INTO_NEW_PARTITION_TABLE));
            }
        }
    }

    @Test
    public void testInsertIntoExistingPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : fixture.getCreateTableFormats()) {
            try {
                doInsertIntoExistingPartition(storageFormat, getTable(INSERT_INTO_EXISTING_PARTITION_TABLE));
            }
            finally {
                dropTable(getTable(INSERT_INTO_EXISTING_PARTITION_TABLE));
            }
        }
    }

    @Test
    public void testMetadataDelete()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : fixture.getCreateTableFormats()) {
            try {
                doMetadataDelete(storageFormat, getTable(METADATA_DELETE));
            }
            finally {
                dropTable(getTable(METADATA_DELETE));
            }
        }
    }

    @Test
    public void testSampledTableCreation()
            throws Exception
    {
        try {
            doCreateSampledTable(getTable(CREATE_SAMPLED));
        }
        finally {
            dropTable(getTable(CREATE_SAMPLED));
        }
    }

    @Test
    public void testEmptyTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : fixture.getCreateTableFormats()) {
            try {
                doCreateEmptyTable(getTable(CREATE_EMPTY), storageFormat, CREATE_TABLE_COLUMNS);
            }
            finally {
                dropTable(getTable(CREATE_EMPTY));
            }
        }
    }

    @Test
    public void testViewCreation()
    {
        try {
            verifyViewCreation();
        }
        finally {
            try {
                runtime.getMetadata().dropView(newSession(), getTable(CREATE_VIEW));
            }
            catch (RuntimeException e) {
                // this usually occurs because the view was not created
            }
        }
    }

    @Test
    public void testCreateTableUnsupportedType()
    {
        for (HiveStorageFormat storageFormat : fixture.getCreateTableFormats()) {
            try {
                ConnectorSession session = newSession();
                List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", HYPER_LOG_LOG, false));
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(getTable(INVALID_TABLE), columns, createTableProperties(storageFormat), session.getUser());
                runtime.getMetadata().beginCreateTable(session, tableMetadata);
                fail("create table with unsupported type should fail for storage format " + storageFormat);
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            }
        }
    }

    private void createDummyTable(SchemaTableName tableName)
    {
        ConnectorSession session = newSession();
        List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", VARCHAR, false));
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(TEXTFILE), session.getUser());
        ConnectorOutputTableHandle handle = runtime.getMetadata().beginCreateTable(session, tableMetadata);
        runtime.getMetadata().commitCreateTable(session, handle, ImmutableList.of());
    }

    private void verifyViewCreation()
    {
        // replace works for new view
        doCreateView(getTable(CREATE_VIEW), true);

        // replace works for existing view
        doCreateView(getTable(CREATE_VIEW), true);

        // create fails for existing view
        try {
            doCreateView(getTable(CREATE_VIEW), false);
            fail("create existing should fail");
        }
        catch (ViewAlreadyExistsException e) {
            assertEquals(e.getViewName(), getTable(CREATE_VIEW));
        }

        // drop works when view exists
        runtime.getMetadata().dropView(newSession(), getTable(CREATE_VIEW));
        assertEquals(runtime.getMetadata().getViews(newSession(), getTable(CREATE_VIEW).toSchemaTablePrefix()).size(), 0);
        assertFalse(runtime.getMetadata().listViews(newSession(), getTable(CREATE_VIEW).getSchemaName()).contains(getTable(CREATE_VIEW)));

        // drop fails when view does not exist
        try {
            runtime.getMetadata().dropView(newSession(), getTable(CREATE_VIEW));
            fail("drop non-existing should fail");
        }
        catch (ViewNotFoundException e) {
            assertEquals(e.getViewName(), getTable(CREATE_VIEW));
        }

        // create works for new view
        doCreateView(getTable(CREATE_VIEW), false);
    }

    private void doCreateView(SchemaTableName viewName, boolean replace)
    {
        String viewData = "test data";

        runtime.getMetadata().createView(newSession(), viewName, viewData, replace);

        Map<SchemaTableName, ConnectorViewDefinition> views = runtime.getMetadata().getViews(newSession(), viewName.toSchemaTablePrefix());
        assertEquals(views.size(), 1);
        assertEquals(views.get(viewName).getViewData(), viewData);

        assertTrue(runtime.getMetadata().listViews(newSession(), viewName.getSchemaName()).contains(viewName));
    }

    protected void doCreateSampledTable(SchemaTableName tableName)
            throws Exception
    {
        ConnectorSession session = newSession();

        // begin creating the table
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("sales", BIGINT, false))
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(RCBINARY), session.getUser(), true);
        ConnectorOutputTableHandle outputHandle = runtime.getMetadata().beginCreateTable(session, tableMetadata);

        // write the records
        ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, outputHandle);

        BlockBuilder sampleBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 3);
        BlockBuilder dataBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 3);

        BIGINT.writeLong(sampleBlockBuilder, 8);
        BIGINT.writeLong(dataBlockBuilder, 2);

        BIGINT.writeLong(sampleBlockBuilder, 5);
        BIGINT.writeLong(dataBlockBuilder, 3);

        BIGINT.writeLong(sampleBlockBuilder, 7);
        BIGINT.writeLong(dataBlockBuilder, 4);

        sink.appendPage(new Page(dataBlockBuilder.build()), sampleBlockBuilder.build());

        Collection<Slice> fragments = sink.commit();

        // commit the table
        runtime.getMetadata().commitCreateTable(session, outputHandle, fragments);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>builder()
                .addAll(runtime.getMetadata().getColumnHandles(session, tableHandle).values())
                .add(runtime.getMetadata().getSampleWeightColumnHandle(session, tableHandle))
                .build();
        assertEquals(columnHandles.size(), 2);

        // verify the metadata
        tableMetadata = runtime.getMetadata().getTableMetadata(session, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), session.getUser());

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);
        assertEquals(columnMap.size(), 1);

        assertPrimitiveField(columnMap, "sales", BIGINT, false);

        // verify the data
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        ConnectorSplitSource splitSource = runtime.getSplitManager().getSplits(session, layoutHandle);
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        try (ConnectorPageSource pageSource = runtime.getPageSourceProvider().createPageSource(session, split, columnHandles)) {
            assertPageSourceType(pageSource, RCBINARY);
            MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
            assertEquals(result.getRowCount(), 3);

            MaterializedRow row;

            row = result.getMaterializedRows().get(0);
            assertEquals(row.getField(0), 2L);
            assertEquals(row.getField(1), 8L);

            row = result.getMaterializedRows().get(1);
            assertEquals(row.getField(0), 3L);
            assertEquals(row.getField(1), 5L);

            row = result.getMaterializedRows().get(2);
            assertEquals(row.getField(0), 4L);
            assertEquals(row.getField(1), 7L);
        }
    }

    protected void doCreateTable(SchemaTableName tableName, HiveStorageFormat storageFormat)
            throws Exception
    {
        ConnectorSession session = newSession();

        // begin creating the table
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, CREATE_TABLE_COLUMNS, createTableProperties(storageFormat), session.getUser());

        ConnectorOutputTableHandle outputHandle = runtime.getMetadata().beginCreateTable(session, tableMetadata);

        // write the data
        ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, outputHandle);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        Collection<Slice> fragments = sink.commit();

        // verify all new files start with the unique prefix
        for (String filePath : listAllDataFiles(outputHandle)) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(outputHandle)));
        }

        // commit the table
        runtime.getMetadata().commitCreateTable(session, outputHandle, fragments);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        // verify the metadata
        tableMetadata = runtime.getMetadata().getTableMetadata(session, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), session.getUser());
        assertEquals(tableMetadata.getColumns(), CREATE_TABLE_COLUMNS);

        // verify the data
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_DATA.getMaterializedRows());
    }

    protected void doCreateEmptyTable(SchemaTableName tableName, HiveStorageFormat storageFormat, List<ColumnMetadata> createTableColumns)
            throws Exception
    {
        ConnectorSession session = newSession();

        List<String> partitionedBy = createTableColumns.stream()
                .filter(ColumnMetadata::isPartitionKey)
                .map(ColumnMetadata::getName)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, createTableColumns, createTableProperties(storageFormat, partitionedBy), session.getUser());

        runtime.getMetadata().createTable(session, tableMetadata);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);

        // verify the metadata
        tableMetadata = runtime.getMetadata().getTableMetadata(session, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), session.getUser());
        assertEquals(tableMetadata.getColumns(), createTableColumns);

        // verify table format
        Table table = runtime.getMetastoreClient().getTable(tableName.getSchemaName(), tableName.getTableName()).get();
        if (!table.getSd().getInputFormat().equals(storageFormat.getInputFormat())) {
            assertEquals(table.getSd().getInputFormat(), storageFormat.getInputFormat());
        }

        // verify the table is empty
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEquals(result.getRowCount(), 0);
    }

    private void doInsert(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            ConnectorSession session = newSession();

            // begin the insert
            ConnectorTableHandle tableHandle = getTableHandle(tableName);
            ConnectorInsertTableHandle insertTableHandle = runtime.getMetadata().beginInsert(session, tableHandle);

            ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, insertTableHandle);

            // write data
            sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
            Collection<Slice> fragments = sink.commit();

            // commit the insert
            runtime.getMetadata().commitInsert(session, insertTableHandle, fragments);

            // load the new table
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

            // verify the metadata
            ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(session, getTableHandle(tableName));
            assertEquals(tableMetadata.getOwner(), session.getUser());
            assertEquals(tableMetadata.getColumns(), CREATE_TABLE_COLUMNS);

            // verify the data
            resultBuilder.rows(CREATE_TABLE_DATA.getMaterializedRows());
            MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
        }

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        ConnectorSession session = newSession();
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = runtime.getMetadata().beginInsert(session, tableHandle);
        ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, insertTableHandle);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        sink.commit();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        runtime.getMetadata().rollbackInsert(session, insertTableHandle);

        // verify the data is unchanged
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
        assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify temp directory is empty
        assertTrue(listAllDataFiles(insertTableHandle).isEmpty());
    }

    // These are protected so extensions to the hive connector can replace the handle classes
    protected String getFilePrefix(ConnectorOutputTableHandle outputTableHandle)
    {
        return ((HiveOutputTableHandle) outputTableHandle).getFilePrefix();
    }

    protected String getFilePrefix(ConnectorInsertTableHandle insertTableHandle)
    {
        return ((HiveInsertTableHandle) insertTableHandle).getFilePrefix();
    }

    protected Set<String> listAllDataFiles(ConnectorOutputTableHandle tableHandle)
            throws IOException
    {
        HiveOutputTableHandle hiveOutputTableHandle = (HiveOutputTableHandle) tableHandle;
        Path writePath = new Path(runtime.getLocationService().writePathRoot(hiveOutputTableHandle.getLocationHandle()).get().toString());
        return listAllDataFiles(writePath);
    }

    protected Set<String> listAllDataFiles(ConnectorInsertTableHandle tableHandle)
            throws IOException
    {
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) tableHandle;
        Path writePath = new Path(runtime.getLocationService().writePathRoot(hiveInsertTableHandle.getLocationHandle()).get().toString());
        return listAllDataFiles(writePath);
    }

    protected Set<String> listAllDataFiles(String schemaName, String tableName)
            throws IOException
    {
        Set<String> existingFiles = new HashSet<>();
        for (String location : listAllDataPaths(runtime.getMetastoreClient(), schemaName, tableName)) {
            existingFiles.addAll(listAllDataFiles(new Path(location)));
        }
        return existingFiles;
    }

    public static List<String> listAllDataPaths(HiveMetastore metastore, String schemaName, String tableName)
    {
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        Table table = metastore.getTable(schemaName, tableName).get();
        if (table.getSd().getLocation() != null) {
            // For unpartitioned table, there should be nothing directly under this directory.
            // But including this location in the set makes the directory content assert more
            // extensive, which is desirable.
            locations.add(table.getSd().getLocation());
        }

        Optional<List<String>> partitionNames = metastore.getPartitionNames(schemaName, tableName);
        if (partitionNames.isPresent()) {
            metastore.getPartitionsByNames(schemaName, tableName, partitionNames.get()).get().values().stream()
                    .map(partition -> partition.getSd().getLocation())
                    .filter(location -> !location.startsWith(table.getSd().getLocation()))
                    .forEach(locations::add);
        }

        return locations.build();
    }

    protected Set<String> listAllDataFiles(Path path)
            throws IOException
    {
        Set<String> result = new HashSet<>();
        FileSystem fileSystem = runtime.getHdfsEnvironment().getFileSystem(path);
        if (fileSystem.exists(path)) {
            for (FileStatus fileStatus : fileSystem.listStatus(path)) {
                if (HadoopFileStatus.isFile(fileStatus)) {
                    result.add(fileStatus.getPath().toString());
                }
                else if (HadoopFileStatus.isDirectory(fileStatus)) {
                    result.addAll(listAllDataFiles(fileStatus.getPath()));
                }
            }
        }
        return result;
    }

    private void doInsertIntoNewPartition(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        ConnectorTableHandle tableHandle = getTableHandle(tableName);

        // insert the data
        insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, newSession());

        // verify partitions were created
        List<String> partitionNames = runtime.getMetastoreClient().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
        assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                .collect(toList()));

        // load the new table
        ConnectorSession session = newSession();
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        // verify the data
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        session = newSession();

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = runtime.getMetadata().beginInsert(session, tableHandle);
        ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, insertTableHandle);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA_2ND.toPage(), null);
        sink.commit();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        runtime.getMetadata().rollbackInsert(session, insertTableHandle);

        // verify the data is unchanged
        result = readTable(tableHandle, columnHandles, newSession(), TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
        assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify temp directory is empty
        assertTrue(listAllDataFiles(insertTableHandle).isEmpty());
    }

    private void doInsertIntoExistingPartition(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            ConnectorTableHandle tableHandle = getTableHandle(tableName);

            // insert the data
            insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, newSession());

            // verify partitions were created
            List<String> partitionNames = runtime.getMetastoreClient().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toList()));

            // load the new table
            ConnectorSession session = newSession();
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

            // verify the data
            resultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());
            MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
        }

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        ConnectorSession session = newSession();
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = runtime.getMetadata().beginInsert(session, tableHandle);
        ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, insertTableHandle);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage(), null);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage(), null);
        sink.commit();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        runtime.getMetadata().rollbackInsert(session, insertTableHandle);

        // verify the data is unchanged
        MaterializedResult result = readTable(tableHandle, columnHandles, newSession(), TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
        assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify temp directory is empty
        assertTrue(listAllDataFiles(insertTableHandle).isEmpty());
    }

    private void insertData(ConnectorTableHandle tableHandle, MaterializedResult data, ConnectorSession session)
    {
        ConnectorInsertTableHandle insertTableHandle = runtime.getMetadata().beginInsert(session, tableHandle);

        ConnectorPageSink sink = runtime.getPageSinkProvider().createPageSink(session, insertTableHandle);

        // write data
        sink.appendPage(data.toPage(), null);
        Collection<Slice> fragments = sink.commit();

        // commit the insert
        runtime.getMetadata().commitInsert(session, insertTableHandle, fragments);
    }

    private void doMetadataDelete(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        // verify table directory is empty
        Set<String> initialFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertTrue(initialFiles.isEmpty());

        MaterializedResult.Builder expectedResultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, newSession());
        expectedResultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        // verify partitions were created
        List<String> partitionNames = runtime.getMetastoreClient().getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
        assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                .collect(toList()));

        // verify table directory is not empty
        Set<String> filesAfterInsert = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(filesAfterInsert.isEmpty());

        // verify the data
        ConnectorSession session = newSession();
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), expectedResultBuilder.build().getMaterializedRows());

        // get ds column handle
        Map<String, HiveColumnHandle> columnHandleMap = columnHandles.stream()
                .map(columnHandle -> (HiveColumnHandle) columnHandle)
                .collect(Collectors.toMap(HiveColumnHandle::getName, Function.identity()));
        HiveColumnHandle dsColumnHandle = columnHandleMap.get("ds");
        int dsColumnOrdinalPosition = columnHandles.indexOf(dsColumnHandle);

        // delete ds=2015-07-03
        session = newSession();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumnHandle, NullableValue.of(VARCHAR, utf8Slice("2015-07-03"))));
        Constraint<ColumnHandle> constraint = new Constraint<>(tupleDomain, convertToPredicate(TYPE_MANAGER, tupleDomain));
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, tableHandle, constraint, Optional.empty());
        ConnectorTableLayoutHandle tableLayoutHandle = Iterables.getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        runtime.getMetadata().metadataDelete(session, tableHandle, tableLayoutHandle);
        // verify the data
        session = newSession();
        ImmutableList<MaterializedRow> expectedRows = expectedResultBuilder.build().getMaterializedRows().stream()
                .filter(row -> !"2015-07-03".equals(row.getField(dsColumnOrdinalPosition)))
                .collect(ImmutableCollectors.toImmutableList());
        MaterializedResult actualAfterDelete = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(actualAfterDelete.getMaterializedRows(), expectedRows);

        // delete ds=2015-07-01 and 2015-07-02
        session = newSession();
        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(dsColumnHandle, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("2015-07-01"), true, utf8Slice("2015-07-02"), true)), false)));
        Constraint<ColumnHandle> constraint2 = new Constraint<>(tupleDomain2, convertToPredicate(TYPE_MANAGER, tupleDomain2));
        List<ConnectorTableLayoutResult> tableLayoutResults2 = runtime.getMetadata().getTableLayouts(session, tableHandle, constraint2, Optional.empty());
        ConnectorTableLayoutHandle tableLayoutHandle2 = Iterables.getOnlyElement(tableLayoutResults2).getTableLayout().getHandle();
        runtime.getMetadata().metadataDelete(session, tableHandle, tableLayoutHandle2);
        // verify the data
        session = newSession();
        MaterializedResult actualAfterDelete2 = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(actualAfterDelete2.getMaterializedRows(), ImmutableList.of());

        // verify table directory is empty
        Set<String> filesAfterDelete = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertTrue(filesAfterDelete.isEmpty());
    }

    protected void assertGetRecordsOptional(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        if (runtime.getMetadata().getTableHandle(newSession(), new SchemaTableName(fixture.getDatabaseName(), tableName)) != null) {
            assertGetRecords(tableName, hiveStorageFormat);
        }
    }

    protected void assertGetRecords(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(fixture.getDatabaseName(), tableName));
        ConnectorTableMetadata tableMetadata = runtime.getMetadata().getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);

        List<ColumnHandle> columnHandles = ImmutableList.copyOf(runtime.getMetadata().getColumnHandles(session, tableHandle).values());

        ConnectorPageSource pageSource = runtime.getPageSourceProvider().createPageSource(session, hiveSplit, columnHandles);
        assertGetRecords(hiveStorageFormat, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    protected HiveSplit getHiveSplit(ConnectorTableHandle tableHandle)
            throws InterruptedException
    {
        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 1);
        return checkType(getOnlyElement(splits), HiveSplit.class, "split");
    }

    protected void assertGetRecords(
            HiveStorageFormat hiveStorageFormat,
            ConnectorTableMetadata tableMetadata,
            HiveSplit hiveSplit,
            ConnectorPageSource pageSource,
            List<? extends ColumnHandle> columnHandles)
            throws IOException
    {
        try {
            MaterializedResult result = materializeSourceDataStream(newSession(), pageSource, getTypes(columnHandles));

            assertPageSourceType(pageSource, hiveStorageFormat);

            ImmutableMap<String, Integer> columnIndex = indexColumns(tableMetadata);

            long rowNumber = 0;
            long completedBytes = 0;
            for (MaterializedRow row : result) {
                try {
                    assertValueTypes(row, tableMetadata.getColumns());
                }
                catch (RuntimeException e) {
                    throw new RuntimeException("row " + rowNumber, e);
                }

                rowNumber++;
                Integer index;

                // STRING
                index = columnIndex.get("t_string");
                if ((rowNumber % 19) == 0) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), ((rowNumber % 19) == 1) ? "" : "test");
                }

                // NUMBERS
                assertEquals(row.getField(columnIndex.get("t_tinyint")), 1 + rowNumber);
                assertEquals(row.getField(columnIndex.get("t_smallint")), 2 + rowNumber);
                assertEquals(row.getField(columnIndex.get("t_int")), 3 + rowNumber);

                index = columnIndex.get("t_bigint");
                if ((rowNumber % 13) == 0) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), 4 + rowNumber);
                }

                assertEquals((Double) row.getField(columnIndex.get("t_float")), 5.1 + rowNumber, 0.001);
                assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);

                // BOOLEAN
                index = columnIndex.get("t_boolean");
                if ((rowNumber % 3) == 2) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), (rowNumber % 3) != 0);
                }

                // TIMESTAMP
                index = columnIndex.get("t_timestamp");
                if (index != null) {
                    if ((rowNumber % 17) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        SqlTimestamp expected = new SqlTimestamp(new DateTime(2011, 5, 6, 7, 8, 9, 123, runtime.getTimeZone()).getMillis(), UTC_KEY);
                        assertEquals(row.getField(index), expected);
                    }
                }

                // BINARY
                index = columnIndex.get("t_binary");
                if (index != null) {
                    if ((rowNumber % 23) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), new SqlVarbinary("test binary".getBytes(UTF_8)));
                    }
                }

                // DATE
                index = columnIndex.get("t_date");
                if (index != null) {
                    if ((rowNumber % 37) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        SqlDate expected = new SqlDate(Ints.checkedCast(TimeUnit.MILLISECONDS.toDays(new DateTime(2013, 8, 9, 0, 0, 0, DateTimeZone.UTC).getMillis())));
                        assertEquals(row.getField(index), expected);
                    }
                }

                /* TODO: enable these tests when the types are supported
                // VARCHAR(50)
                index = columnIndex.get("t_varchar");
                if (index != null) {
                    if ((rowNumber % 39) == 0) {
                        assertTrue(cursor.isNull(index));
                    }
                    else {
                        String stringValue = cursor.getSlice(index).toStringUtf8();
                        assertEquals(stringValue, ((rowNumber % 39) == 1) ? "" : "test varchar");
                    }
                }

                // CHAR(25)
                index = columnIndex.get("t_char");
                if (index != null) {
                    if ((rowNumber % 41) == 0) {
                        assertTrue(cursor.isNull(index));
                    }
                    else {
                        String stringValue = cursor.getSlice(index).toStringUtf8();
                        assertEquals(stringValue, ((rowNumber % 41) == 1) ? "" : "test char");
                    }
                }
                */

                // MAP<STRING, STRING>
                index = columnIndex.get("t_map");
                if (index != null) {
                    if ((rowNumber % 27) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), ImmutableMap.of("test key", "test value"));
                    }
                }

                // ARRAY<STRING>
                index = columnIndex.get("t_array_string");
                if (index != null) {
                    if ((rowNumber % 29) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), ImmutableList.of("abc", "xyz", "data"));
                    }
                }

                // ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>
                index = columnIndex.get("t_array_struct");
                if (index != null) {
                    if ((rowNumber % 31) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        List<Object> expected1 = ImmutableList.<Object>of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.<Object>of("test xyz", 0.2);
                        assertEquals(row.getField(index), ImmutableList.of(expected1, expected2));
                    }
                }

                // MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
                index = columnIndex.get("t_complex");
                if (index != null) {
                    if ((rowNumber % 33) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        List<Object> expected1 = ImmutableList.<Object>of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.<Object>of("test xyz", 0.2);
                        assertEquals(row.getField(index), ImmutableMap.of(1L, ImmutableList.of(expected1, expected2)));
                    }
                }

                // NEW COLUMN
                assertNull(row.getField(columnIndex.get("new_column")));

                long newCompletedBytes = pageSource.getCompletedBytes();
                assertTrue(newCompletedBytes >= completedBytes);
                assertTrue(newCompletedBytes <= hiveSplit.getLength());
                completedBytes = newCompletedBytes;
            }

            assertTrue(completedBytes <= hiveSplit.getLength());
            assertEquals(rowNumber, 100);
        }
        finally {
            pageSource.close();
        }
    }

    protected void dropTable(SchemaTableName table)
    {
        try {
            ConnectorSession session = newSession();

            ConnectorTableHandle handle = runtime.getMetadata().getTableHandle(session, table);
            if (handle == null) {
                return;
            }

            runtime.getMetadata().dropTable(session, handle);
            try {
                // todo I have no idea why this is needed... maybe there is a propagation delay in the metastore?
                runtime.getMetadata().dropTable(session, handle);
                fail("expected NotFoundException");
            }
            catch (TableNotFoundException expected) {
            }
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    protected ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = runtime.getMetadata().getTableHandle(newSession(), tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private MaterializedResult readTable(
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            ConnectorSession session,
            TupleDomain<ColumnHandle> tupleDomain,
            OptionalInt expectedSplitCount,
            Optional<HiveStorageFormat> expectedStorageFormat)
            throws Exception
    {
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(runtime.getSplitManager().getSplits(session, layoutHandle));
        if (expectedSplitCount.isPresent()) {
            assertEquals(splits.size(), expectedSplitCount.getAsInt());
        }

        ImmutableList.Builder<MaterializedRow> allRows = ImmutableList.builder();
        for (ConnectorSplit split : splits) {
            try (ConnectorPageSource pageSource = runtime.getPageSourceProvider().createPageSource(session, split, columnHandles)) {
                if (expectedStorageFormat.isPresent()) {
                    assertPageSourceType(pageSource, expectedStorageFormat.get());
                }
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                allRows.addAll(result.getMaterializedRows());
            }
        }
        return new MaterializedResult(allRows.build(), getTypes(columnHandles));
    }

    protected static int getSplitCount(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = getFutureValue(splitSource.getNextBatch(1000));
            splitCount += batch.size();
        }
        return splitCount;
    }

    private List<ConnectorSplit> getAllSplits(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
            throws InterruptedException
    {
        ConnectorSession session = newSession();
        List<ConnectorTableLayoutResult> tableLayoutResults = runtime.getMetadata().getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        return getAllSplits(runtime.getSplitManager().getSplits(session, layoutHandle));
    }

    protected static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = getFutureValue(splitSource.getNextBatch(1000));
            splits.addAll(batch);
        }
        return splits.build();
    }

    protected List<?> getAllPartitions(ConnectorTableLayoutHandle layoutHandle)
    {
        return ((HiveTableLayoutHandle) layoutHandle).getPartitions().get();
    }

    protected String getPartitionId(Object partition)
    {
        return ((HivePartition) partition).getPartitionId();
    }

    protected static void assertPageSourceType(ConnectorPageSource pageSource, HiveStorageFormat hiveStorageFormat)
    {
        if (pageSource instanceof RecordPageSource) {
            assertInstanceOf(((RecordPageSource) pageSource).getCursor(), recordCursorType(hiveStorageFormat), hiveStorageFormat.name());
        }
        else {
            assertInstanceOf(pageSource, pageSourceType(hiveStorageFormat), hiveStorageFormat.name());
        }
    }

    private static Class<? extends HiveRecordCursor> recordCursorType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
            case RCTEXT:
                return ColumnarTextHiveRecordCursor.class;
            case RCBINARY:
                return ColumnarBinaryHiveRecordCursor.class;
            case ORC:
                return OrcHiveRecordCursor.class;
            case PARQUET:
                return ParquetHiveRecordCursor.class;
            case DWRF:
                return DwrfHiveRecordCursor.class;
        }
        return GenericHiveRecordCursor.class;
    }

    private static Class<? extends ConnectorPageSource> pageSourceType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
            case RCTEXT:
            case RCBINARY:
                return RcFilePageSource.class;
            case ORC:
            case DWRF:
                return OrcPageSource.class;
            default:
                throw new AssertionError("Filed type " + hiveStorageFormat + " does not use a page source");
        }
    }

    private static void assertValueTypes(MaterializedRow row, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            Object value = row.getField(columnIndex);
            if (value != null) {
                if (BOOLEAN.equals(column.getType())) {
                    assertInstanceOf(value, Boolean.class);
                }
                else if (BIGINT.equals(column.getType())) {
                    assertInstanceOf(value, Long.class);
                }
                else if (DOUBLE.equals(column.getType())) {
                    assertInstanceOf(value, Double.class);
                }
                else if (VARCHAR.equals(column.getType())) {
                    assertInstanceOf(value, String.class);
                }
                else if (VARBINARY.equals(column.getType())) {
                    assertInstanceOf(value, SqlVarbinary.class);
                }
                else if (TIMESTAMP.equals(column.getType())) {
                    assertInstanceOf(value, SqlTimestamp.class);
                }
                else if (DATE.equals(column.getType())) {
                    assertInstanceOf(value, SqlDate.class);
                }
                else if (column.getType() instanceof ArrayType) {
                    assertInstanceOf(value, List.class);
                }
                else if (column.getType() instanceof MapType) {
                    assertInstanceOf(value, Map.class);
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private static void assertPrimitiveField(Map<String, ColumnMetadata> map, String name, Type type, boolean partitionKey)
    {
        assertTrue(map.containsKey(name));
        ColumnMetadata column = map.get(name);
        assertEquals(column.getType(), type, name);
        assertEquals(column.isPartitionKey(), partitionKey, name);
    }

    protected static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            HiveColumnHandle hiveColumnHandle = checkType(columnHandle, HiveColumnHandle.class, "columnHandle");
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.build();
    }

    protected static ImmutableMap<String, Integer> indexColumns(ConnectorTableMetadata tableMetadata)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            index.put(columnMetadata.getName(), i);
            i++;
        }
        return index.build();
    }

    private static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat)
    {
        return createTableProperties(storageFormat, ImmutableList.of());
    }

    private static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat, Iterable<String> parititonedBy)
    {
        return ImmutableMap.<String, Object>builder()
                .put(STORAGE_FORMAT_PROPERTY, storageFormat)
                .put(PARTITIONED_BY_PROPERTY, ImmutableList.copyOf(parititonedBy))
                .build();
    }
}
