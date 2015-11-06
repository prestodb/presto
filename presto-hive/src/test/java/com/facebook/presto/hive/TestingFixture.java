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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

public class TestingFixture
{
    public static final String INVALID_TABLE = "totally_invalid_table_name";
    public static final String UNPARTITIONED_TABLE = "presto_test_unpartitioned";

    public static final String INVALID_DATABASE = "totally_invalid_database_name";
    public static final String INVALID_COLUMN = "totally_invalid_column_name";
    public static final String OFFLINE_TABLE = "presto_test_offline";
    public static final String OFFLINE_PARTITION = "presto_test_offline_partition";
    public static final String VIEW = "presto_test_view";
    public static final String BUCKETED_BY_STRING_INT = "presto_test_bucketed_by_string_int";
    public static final String BUCKETED_BY_BIGINT_BOOLEAN = "presto_test_bucketed_by_bigint_boolean";
    public static final String BUCKETED_BY_DOUBLE_FLOAT = "presto_test_bucketed_by_double_float";
    public static final String PARTITION_SCHEMA_CHANGE = "presto_test_partition_schema_change";
    public static final String PARTITION_SCHEMA_CHANGE_NON_CANONICAL = "presto_test_partition_schema_change_non_canonical";
    public static final String CREATE = "tmp_presto_test_create_" + randomName();
    public static final String CREATE_SAMPLED = "tmp_presto_test_create_" + randomName();
    public static final String CREATE_EMPTY = "tmp_presto_test_create_" + randomName();
    public static final String INSERT = "tmp_presto_test_insert_" + randomName();
    public static final String METADATA_DELETE = "tmp_presto_test_metadata_delete_" + randomName();
    public static final String RENAME_OLD = "tmp_presto_test_rename_" + randomName();
    public static final String RENAME_NEW = "tmp_presto_test_rename_" + randomName();
    public static final String CREATE_VIEW = "tmp_presto_test_create_" + randomName();
    public static final String INSERT_INTO_EXISTING_PARTITION_TABLE = "tmp_presto_test_insert_exsting_partitioned_" + randomName();
    public static final String INSERT_INTO_NEW_PARTITION_TABLE = "tmp_presto_test_insert_new_partitioned_" + randomName();

    public static final Type ARRAY_TYPE = TYPE_MANAGER.getParameterizedType(ARRAY, ImmutableList.of(VARCHAR.getTypeSignature()), ImmutableList.of());
    public static final Type MAP_TYPE = TYPE_MANAGER.getParameterizedType(MAP, ImmutableList.of(VARCHAR.getTypeSignature(), BIGINT.getTypeSignature()), ImmutableList.of());
    public static final Type ROW_TYPE = TYPE_MANAGER.getParameterizedType(
            ROW,
            ImmutableList.of(VARCHAR.getTypeSignature(), BIGINT.getTypeSignature(), BOOLEAN.getTypeSignature()),
            ImmutableList.of("f_string", "f_bigint", "f_boolean"));

    public static final List<ColumnMetadata> CREATE_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("id", BIGINT, false))
            .add(new ColumnMetadata("t_string", VARCHAR, false))
            .add(new ColumnMetadata("t_bigint", BIGINT, false))
            .add(new ColumnMetadata("t_double", DOUBLE, false))
            .add(new ColumnMetadata("t_boolean", BOOLEAN, false))
            .add(new ColumnMetadata("t_array", ARRAY_TYPE, false))
            .add(new ColumnMetadata("t_map", MAP_TYPE, false))
            .add(new ColumnMetadata("t_row", ROW_TYPE, false))
            .build();

    public static final MaterializedResult CREATE_TABLE_DATA = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE)
            .row(1, "hello", 123, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1, true))
            .row(2, null, null, null, null, null, null, null)
            .row(3, "bye", 456, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0, false))
            .build();

    public static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata("ds", VARCHAR, true))
            .build();

    public static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA = new MaterializedResult(
            CREATE_TABLE_DATA.getMaterializedRows().stream()
                    .map(row -> new MaterializedRow(row.getPrecision(), newArrayList(concat(row.getFields(), ImmutableList.of("2015-07-0" + row.getField(0))))))
                    .collect(toList()),
            ImmutableList.<Type>builder()
                    .addAll(CREATE_TABLE_DATA.getTypes())
                    .add(VARCHAR)
                    .build());

    public static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA_2ND = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE, VARCHAR)
            .row(4, "hello", 123, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1, true), "2015-07-04")
            .row(5, null, null, null, null, null, null, null, "2015-07-04")
            .row(6, "bye", 456, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0, false), "2015-07-04")
            .build();

    private final String connectorId;
    private final ColumnHandle dsColumn;
    private final int partitionCount;
    private final SchemaTableName tablePartitionFormat;
    private final ConnectorTableHandle invalidTableHandle;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final ConnectorTableLayout tableLayout;
    private final ConnectorTableLayout unpartitionedTableLayout;
    private final ConnectorTableLayoutHandle invalidTableLayoutHandle;
    private final String databaseName;
    private final ConnectorTableLayoutHandle emptyTableLayoutHandle;
    private final Set<HiveStorageFormat> createTableFormats;
    private final HiveColumnHandle invalidColumnHandle;
    private final HiveColumnHandle intColumn;

    public TestingFixture(
            String connectorId,
            String databaseName,
            ConnectorTableLayoutHandle emptyTableLayoutHandle,
            SchemaTableName tablePartitionFormat,
            ConnectorTableHandle invalidTableHandle,
            TupleDomain<ColumnHandle> tupleDomain,
            ConnectorTableLayout tableLayout,
            ConnectorTableLayout unpartitionedTableLayout,
            ConnectorTableLayoutHandle invalidTableLayoutHandle,
            Set<HiveStorageFormat> createTableFormats,
            ColumnHandle dsColumn,
            int partitionCount)
    {
        this.connectorId = connectorId;
        this.databaseName = databaseName;
        this.emptyTableLayoutHandle = emptyTableLayoutHandle;
        this.tablePartitionFormat = tablePartitionFormat;
        this.invalidTableHandle = invalidTableHandle;
        this.tupleDomain = tupleDomain;
        this.tableLayout = tableLayout;
        this.unpartitionedTableLayout = unpartitionedTableLayout;
        this.invalidTableLayoutHandle = invalidTableLayoutHandle;
        this.createTableFormats = createTableFormats;
        this.dsColumn = dsColumn;
        this.partitionCount = partitionCount;

        intColumn = new HiveColumnHandle(connectorId, "t_int", HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 0, false);
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public Set<HiveStorageFormat> getCreateTableFormats()
    {
        return createTableFormats;
    }

    public ConnectorTableLayoutHandle getEmptyTableLayoutHandle()
    {
        return emptyTableLayoutHandle;
    }

    public ConnectorTableHandle getInvalidTableHandle()
    {
        return invalidTableHandle;
    }

    public ConnectorTableLayoutHandle getInvalidTableLayoutHandle()
    {
        return invalidTableLayoutHandle;
    }

    public ConnectorTableLayout getTableLayout()
    {
        return tableLayout;
    }

    public SchemaTableName getTablePartitionFormat()
    {
        return tablePartitionFormat;
    }

    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    public ConnectorTableLayout getUnpartitionedTableLayout()
    {
        return unpartitionedTableLayout;
    }

    public int getPartitionCount()
    {
        return partitionCount;
    }

    public ColumnHandle getDsColumn()
    {
        return dsColumn;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    public HiveColumnHandle getIntColumn()
    {
        return intColumn;
    }

    public HiveColumnHandle getInvalidColumnHandle()
    {
        return invalidColumnHandle;
    }

    public static TestingFixture newInstance(String connectorId, String databaseName)
    {
        HiveColumnHandle dsColumn = new HiveColumnHandle(connectorId, "ds", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);

        SchemaTableName tablePartitionFormat = new SchemaTableName(databaseName, "presto_test_partition_format");
        ConnectorTableHandle invalidTableHandle = new HiveTableHandle("hive", databaseName, INVALID_TABLE);
        ConnectorTableLayoutHandle invalidTableLayoutHandle = new HiveTableLayoutHandle("hive",
                ImmutableList.of(new HivePartition(new SchemaTableName(databaseName, INVALID_TABLE), TupleDomain.all(), "unknown", ImmutableMap.of(), Optional.empty())),
                TupleDomain.all());
        ConnectorTableLayoutHandle emptyTableLayoutHandle = new HiveTableLayoutHandle("hive", ImmutableList.of(), TupleDomain.none());
        ColumnHandle fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);
        ColumnHandle dummyColumn = new HiveColumnHandle(connectorId, "dummy", HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);

        List<HivePartition> partitions = ImmutableList.<HivePartition>builder()
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=textfile/dummy=1",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("textfile")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 1L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=sequencefile/dummy=2",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("sequencefile")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 2L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rctext/dummy=3",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("rctext")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 3L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rcbinary/dummy=4",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("rcbinary")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 4L))
                                .build(),
                        Optional.empty()))
                .build();
        int partitionCount = partitions.size();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29"))));
        ConnectorTableLayout tableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(connectorId, partitions, tupleDomain),
                Optional.empty(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                        fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("textfile")), Range.equal(VARCHAR, utf8Slice("sequencefile")), Range.equal(VARCHAR, utf8Slice("rctext")), Range.equal(VARCHAR, utf8Slice("rcbinary"))), false),
                        dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L), Range.equal(BIGINT, 4L)), false))),
                Optional.empty(),
                Optional.of(ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("textfile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("sequencefile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("rctext"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 3L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("rcbinary"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 4L)), false)))
                )),
                ImmutableList.of());
        List<HivePartition> unpartitionedPartitions = ImmutableList.of(new HivePartition(new SchemaTableName(databaseName, UNPARTITIONED_TABLE), TupleDomain.<HiveColumnHandle>all()));
        ConnectorTableLayout unpartitionedTableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(connectorId, unpartitionedPartitions, TupleDomain.all()),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.of(ImmutableList.of(TupleDomain.all())),
                ImmutableList.of());

        return new TestingFixture(
                connectorId,
                databaseName,
                emptyTableLayoutHandle,
                tablePartitionFormat,
                invalidTableHandle,
                tupleDomain,
                tableLayout,
                unpartitionedTableLayout,
                invalidTableLayoutHandle,
                ImmutableSet.copyOf(HiveStorageFormat.values()),
                dsColumn,
                partitionCount);
    }

    public static String randomName()
    {
        return UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
    }
}
