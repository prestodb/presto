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

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.hadoop.HadoopFileStatus;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.ThriftHiveMetastore;
import com.facebook.presto.hive.orc.OrcPageSource;
import com.facebook.presto.hive.parquet.ParquetHiveRecordCursor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
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
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.TestException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.COMMIT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_APPEND_PAGE;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_BEGIN_INSERT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_DELETE;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_FINISH_INSERT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_SINK_FINISH;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_RIGHT_AWAY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveMetadata.convertToPredicate;
import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.SEQUENCEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveDataStreamFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveUtil.annotateColumnComment;
import static com.facebook.presto.hive.HiveWriteUtils.createDirectory;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.difference;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
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
    protected static final String INVALID_DATABASE = "totally_invalid_database_name";
    protected static final String INVALID_TABLE = "totally_invalid_table_name";
    protected static final String INVALID_COLUMN = "totally_invalid_column_name";

    protected static final String TEST_SERVER_VERSION = "test_version";

    private static final Type ARRAY_TYPE = TYPE_MANAGER.getParameterizedType(ARRAY, ImmutableList.of(TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature())));
    private static final Type MAP_TYPE = TYPE_MANAGER.getParameterizedType(MAP, ImmutableList.of(TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature()), TypeSignatureParameter.of(BIGINT.getTypeSignature())));
    private static final Type ROW_TYPE = TYPE_MANAGER.getParameterizedType(
            ROW,
            ImmutableList.of(
                    TypeSignatureParameter.of(new NamedTypeSignature("f_string", createUnboundedVarcharType().getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature("f_bigint", BIGINT.getTypeSignature())),
                    TypeSignatureParameter.of(new NamedTypeSignature("f_boolean", BOOLEAN.getTypeSignature())))
    );

    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("id", BIGINT))
            .add(new ColumnMetadata("t_string", createUnboundedVarcharType()))
            .add(new ColumnMetadata("t_tinyint", TINYINT))
            .add(new ColumnMetadata("t_smallint", SMALLINT))
            .add(new ColumnMetadata("t_integer", INTEGER))
            .add(new ColumnMetadata("t_bigint", BIGINT))
            .add(new ColumnMetadata("t_float", REAL))
            .add(new ColumnMetadata("t_double", DOUBLE))
            .add(new ColumnMetadata("t_boolean", BOOLEAN))
            .add(new ColumnMetadata("t_array", ARRAY_TYPE))
            .add(new ColumnMetadata("t_map", MAP_TYPE))
            .add(new ColumnMetadata("t_row", ROW_TYPE))
            .build();

    private static final MaterializedResult CREATE_TABLE_DATA =
            MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE)
                    .row(1L, "hello", (byte) 45, (short) 345, 234, 123L, -754.1985f, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1L, true))
                    .row(2L, null, null, null, null, null, null, null, null, null, null, null)
                    .row(3L, "bye", (byte) 46, (short) 346, 345, 456L, 754.2008f, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0L, false))
                    .build();

    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata("ds", createUnboundedVarcharType()))
            .build();

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA = new MaterializedResult(
            CREATE_TABLE_DATA.getMaterializedRows().stream()
                    .map(row -> new MaterializedRow(row.getPrecision(), newArrayList(concat(row.getFields(), ImmutableList.of("2015-07-0" + row.getField(0))))))
                    .collect(toList()),
            ImmutableList.<Type>builder()
                    .addAll(CREATE_TABLE_DATA.getTypes())
                    .add(createUnboundedVarcharType())
                    .build());

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA_2ND =
            MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE, createUnboundedVarcharType())
                    .row(4L, "hello", (byte) 45, (short) 345, 234, 123L, 754.1985f, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1L, true), "2015-07-04")
                    .row(5L, null, null, null, null, null, null, null, null, null, null, null, "2015-07-04")
                    .row(6L, "bye", (byte) 46, (short) 346, 345, 456L, -754.2008f, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0L, false), "2015-07-04")
                    .build();

    protected Set<HiveStorageFormat> createTableFormats = difference(ImmutableSet.copyOf(HiveStorageFormat.values()), ImmutableSet.of(AVRO));

    protected String clientId;
    protected String database;
    protected SchemaTableName tablePartitionFormat;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName tableOffline;
    protected SchemaTableName tableOfflinePartition;
    protected SchemaTableName view;
    protected SchemaTableName invalidTable;
    protected SchemaTableName tableBucketedStringInt;
    protected SchemaTableName tableBucketedBigintBoolean;
    protected SchemaTableName tableBucketedDoubleFloat;
    protected SchemaTableName tablePartitionSchemaChange;
    protected SchemaTableName tablePartitionSchemaChangeNonCanonical;

    protected SchemaTableName temporaryCreateTable;
    protected SchemaTableName temporaryCreateRollbackTable;
    protected SchemaTableName temporaryCreateEmptyTable;
    protected SchemaTableName temporaryInsertTable;
    protected SchemaTableName temporaryInsertIntoNewPartitionTable;
    protected SchemaTableName temporaryInsertIntoExistingPartitionTable;
    protected SchemaTableName temporaryInsertUnsupportedWriteType;
    protected SchemaTableName temporaryMetadataDeleteTable;
    protected SchemaTableName temporaryRenameTableOld;
    protected SchemaTableName temporaryRenameTableNew;
    protected SchemaTableName temporaryCreateView;
    protected SchemaTableName temporaryDeleteInsert;

    protected String invalidClientId;
    protected ConnectorTableHandle invalidTableHandle;

    protected ColumnHandle dsColumn;
    protected ColumnHandle fileFormatColumn;
    protected ColumnHandle dummyColumn;
    protected ColumnHandle intColumn;
    protected ColumnHandle invalidColumnHandle;

    protected int partitionCount;
    protected TupleDomain<ColumnHandle> tupleDomain;
    protected ConnectorTableLayout tableLayout;
    protected ConnectorTableLayout unpartitionedTableLayout;
    protected ConnectorTableLayoutHandle invalidTableLayoutHandle;
    protected ConnectorTableLayoutHandle emptyTableLayoutHandle;

    protected DateTimeZone timeZone;

    protected HdfsEnvironment hdfsEnvironment;
    protected LocationService locationService;

    protected HiveMetadataFactory metadataFactory;
    protected HiveTransactionManager transactionManager;
    protected ExtendedHiveMetastore metastoreClient;
    protected ConnectorSplitManager splitManager;
    protected ConnectorPageSourceProvider pageSourceProvider;
    protected ConnectorPageSinkProvider pageSinkProvider;
    protected ExecutorService executor;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    protected void setupHive(String connectorId, String databaseName, String timeZoneId)
    {
        clientId = connectorId;
        database = databaseName;
        tablePartitionFormat = new SchemaTableName(database, "presto_test_partition_format");
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        tableOffline = new SchemaTableName(database, "presto_test_offline");
        tableOfflinePartition = new SchemaTableName(database, "presto_test_offline_partition");
        view = new SchemaTableName(database, "presto_test_view");
        invalidTable = new SchemaTableName(database, INVALID_TABLE);
        tableBucketedStringInt = new SchemaTableName(database, "presto_test_bucketed_by_string_int");
        tableBucketedBigintBoolean = new SchemaTableName(database, "presto_test_bucketed_by_bigint_boolean");
        tableBucketedDoubleFloat = new SchemaTableName(database, "presto_test_bucketed_by_double_float");
        tablePartitionSchemaChange = new SchemaTableName(database, "presto_test_partition_schema_change");
        tablePartitionSchemaChangeNonCanonical = new SchemaTableName(database, "presto_test_partition_schema_change_non_canonical");

        temporaryCreateTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryCreateRollbackTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryCreateEmptyTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryInsertTable = new SchemaTableName(database, "tmp_presto_test_insert_" + randomName());
        temporaryInsertIntoExistingPartitionTable = new SchemaTableName(database, "tmp_presto_test_insert_exsting_partitioned_" + randomName());
        temporaryInsertIntoNewPartitionTable = new SchemaTableName(database, "tmp_presto_test_insert_new_partitioned_" + randomName());
        temporaryInsertUnsupportedWriteType = new SchemaTableName(database, "tmp_presto_test_insert_unsupported_type_" + randomName());
        temporaryMetadataDeleteTable = new SchemaTableName(database, "tmp_presto_test_metadata_delete_" + randomName());
        temporaryRenameTableOld = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryRenameTableNew = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryCreateView = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryDeleteInsert = new SchemaTableName(database, "tmp_presto_test_delete_insert_" + randomName());

        invalidClientId = "hive";
        invalidTableHandle = new HiveTableHandle(invalidClientId, database, INVALID_TABLE);
        invalidTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId,
                ImmutableList.of(),
                ImmutableList.of(new HivePartition(invalidTable, TupleDomain.all(), "unknown", ImmutableMap.of(), Optional.empty())),
                TupleDomain.all(),
                Optional.empty());
        emptyTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId, ImmutableList.of(), ImmutableList.of(), TupleDomain.none(), Optional.empty());

        dsColumn = new HiveColumnHandle(connectorId, "ds", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, PARTITION_KEY);
        fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, PARTITION_KEY);
        dummyColumn = new HiveColumnHandle(connectorId, "dummy", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), -1, PARTITION_KEY);
        intColumn = new HiveColumnHandle(connectorId, "t_int", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), -1, PARTITION_KEY);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 0, REGULAR);

        List<ColumnHandle> partitionColumns = ImmutableList.of(dsColumn, fileFormatColumn, dummyColumn);
        List<HivePartition> partitions = ImmutableList.<HivePartition>builder()
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=textfile/dummy=1",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("textfile")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 1L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=sequencefile/dummy=2",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("sequencefile")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 2L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rctext/dummy=3",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("rctext")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 3L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rcbinary/dummy=4",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("rcbinary")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 4L))
                                .build(),
                        Optional.empty()))
                .build();
        partitionCount = partitions.size();
        tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29"))));
        tableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(clientId, partitionColumns, partitions, tupleDomain, Optional.empty()),
                Optional.empty(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                        fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("textfile")), Range.equal(createUnboundedVarcharType(), utf8Slice("sequencefile")), Range.equal(createUnboundedVarcharType(), utf8Slice("rctext")), Range.equal(createUnboundedVarcharType(), utf8Slice("rcbinary"))), false),
                        dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 1L), Range.equal(INTEGER, 2L), Range.equal(INTEGER, 3L), Range.equal(INTEGER, 4L)), false))),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DiscretePredicates(partitionColumns, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("textfile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 1L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("sequencefile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 2L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("rctext"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 3L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(createUnboundedVarcharType(), utf8Slice("rcbinary"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 4L)), false)))
                ))),
                ImmutableList.of());
        List<HivePartition> unpartitionedPartitions = ImmutableList.of(new HivePartition(tableUnpartitioned, TupleDomain.all()));
        unpartitionedTableLayout = new ConnectorTableLayout(new HiveTableLayoutHandle(clientId, ImmutableList.of(), unpartitionedPartitions, TupleDomain.all(), Optional.empty()));
        timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneId));
    }

    protected final void setup(String host, int port, String databaseName, String timeZone)
    {
        setup(host, port, databaseName, timeZone, "hive-test", 100, 50);
    }

    protected final void setup(String host, int port, String databaseName, String timeZoneId, String connectorName, int maxOutstandingSplits, int maxThreads)
    {
        setupHive(connectorName, databaseName, timeZoneId);

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        hiveClientConfig.setTimeZone(timeZoneId);
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        metastoreClient = new CachingHiveMetastore(new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster)), executor, Duration.valueOf("1m"), Duration.valueOf("15s"));
        HiveConnectorId connectorId = new HiveConnectorId(connectorName);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(hiveClientConfig));

        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig, new NoHdfsAuthentication());
        locationService = new HiveLocationService(hdfsEnvironment);
        TypeManager typeManager = new TypeRegistry();
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        metadataFactory = new HiveMetadataFactory(
                connectorId,
                metastoreClient,
                hdfsEnvironment,
                new HivePartitionManager(connectorId, TYPE_MANAGER, hiveClientConfig),
                timeZone,
                10,
                true,
                true,
                false,
                true,
                true,
                HiveStorageFormat.RCBINARY,
                typeManager,
                locationService,
                new TableParameterCodec(),
                partitionUpdateCodec,
                newFixedThreadPool(2),
                new HiveTypeTranslator(),
                TEST_SERVER_VERSION);
        transactionManager = new HiveTransactionManager();
        splitManager = new HiveSplitManager(
                connectorId,
                transactionHandle -> ((HiveMetadata) transactionManager.get(transactionHandle)).getMetastore(),
                new NamenodeStats(),
                hdfsEnvironment,
                new HadoopDirectoryLister(),
                newDirectExecutorService(),
                maxOutstandingSplits,
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxInitialSplits(),
                false
        );
        pageSinkProvider = new HivePageSinkProvider(hdfsEnvironment, metastoreClient, new GroupByHashPageIndexerFactory(), typeManager, new HiveClientConfig(), locationService, partitionUpdateCodec);
        pageSourceProvider = new HivePageSourceProvider(hiveClientConfig, hdfsEnvironment, getDefaultHiveRecordCursorProvider(hiveClientConfig), getDefaultHiveDataStreamFactories(hiveClientConfig), TYPE_MANAGER);
    }

    protected ConnectorSession newSession()
    {
        return new TestingConnectorSession(new HiveSessionProperties(new HiveClientConfig()).getSessionProperties());
    }

    protected Transaction newTransaction()
    {
        return new HiveTransaction(transactionManager, metadataFactory.create());
    }

    interface Transaction
            extends AutoCloseable
    {
        ConnectorMetadata getMetadata();

        SemiTransactionalHiveMetastore getMetastore(String schema);

        ConnectorTransactionHandle getTransactionHandle();

        void commit();

        void rollback();

        @Override
        void close();
    }

    static class HiveTransaction
            implements Transaction
    {
        private final HiveTransactionManager transactionManager;
        private final ConnectorTransactionHandle transactionHandle;
        private boolean closed;

        public HiveTransaction(HiveTransactionManager transactionManager, HiveMetadata hiveMetadata)
        {
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.transactionHandle = new HiveTransactionHandle();
            transactionManager.put(transactionHandle, hiveMetadata);
            getMetastore().testOnlyThrowOnCleanupFailures();
        }

        @Override
        public ConnectorMetadata getMetadata()
        {
            return transactionManager.get(transactionHandle);
        }

        @Override
        public SemiTransactionalHiveMetastore getMetastore(String schema)
        {
            return getMetastore();
        }

        private SemiTransactionalHiveMetastore getMetastore()
        {
            return ((HiveMetadata) transactionManager.get(transactionHandle)).getMetastore();
        }

        @Override
        public ConnectorTransactionHandle getTransactionHandle()
        {
            return transactionHandle;
        }

        @Override
        public void commit()
        {
            checkState(!closed);
            closed = true;
            HiveMetadata metadata = (HiveMetadata) transactionManager.remove(transactionHandle);
            checkArgument(metadata != null, "no such transaction: %s", transactionHandle);
            metadata.commit();
        }

        @Override
        public void rollback()
        {
            checkState(!closed);
            closed = true;
            HiveMetadata metadata = (HiveMetadata) transactionManager.remove(transactionHandle);
            checkArgument(metadata != null, "no such transaction: %s", transactionHandle);
            metadata.rollback();
        }

        @Override
        public void close()
        {
            if (!closed) {
                try {
                    getMetastore().testOnlyCheckIsReadOnly(); // transactions in this test with writes in it must explicitly commit or rollback
                }
                finally {
                    rollback();
                }
            }
        }
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<String> databases = metadata.listSchemaNames(newSession());
            assertTrue(databases.contains(database));
        }
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<SchemaTableName> tables = metadata.listTables(newSession(), database);
            assertTrue(tables.contains(tablePartitionFormat));
            assertTrue(tables.contains(tableUnpartitioned));
        }
    }

    @Test
    public void testGetAllTableNames()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<SchemaTableName> tables = metadata.listTables(newSession(), null);
            assertTrue(tables.contains(tablePartitionFormat));
            assertTrue(tables.contains(tableUnpartitioned));
        }
    }

    @Test
    public void testGetAllTableColumns()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, List<ColumnMetadata>> allColumns = metadata.listTableColumns(newSession(), new SchemaTablePrefix());
            assertTrue(allColumns.containsKey(tablePartitionFormat));
            assertTrue(allColumns.containsKey(tableUnpartitioned));
        }
    }

    @Test
    public void testGetAllTableColumnsInSchema()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, List<ColumnMetadata>> allColumns = metadata.listTableColumns(newSession(), new SchemaTablePrefix(database));
            assertTrue(allColumns.containsKey(tablePartitionFormat));
            assertTrue(allColumns.containsKey(tableUnpartitioned));
        }
    }

    @Test
    public void testListUnknownSchema()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            assertNull(metadata.getTableHandle(session, new SchemaTableName(INVALID_DATABASE, INVALID_TABLE)));
            assertEquals(metadata.listTables(session, INVALID_DATABASE), ImmutableList.of());
            assertEquals(metadata.listTableColumns(session, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
            assertEquals(metadata.listViews(session, INVALID_DATABASE), ImmutableList.of());
            assertEquals(metadata.getViews(session, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
        }
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
        }
    }

    @Test
    public void testGetPartitionsWithBindings()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.withColumnDomains(ImmutableMap.of(intColumn, Domain.singleValue(BIGINT, 5L))), bindings -> true), Optional.empty());
            assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
        }
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.getTableLayouts(newSession(), invalidTableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        }
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
        }
    }

    protected void assertExpectedTableLayout(ConnectorTableLayout actualTableLayout, ConnectorTableLayout expectedTableLayout)
    {
        assertExpectedTableLayoutHandle(actualTableLayout.getHandle(), expectedTableLayout.getHandle());
        assertEquals(actualTableLayout.getPredicate(), expectedTableLayout.getPredicate());
        assertEquals(actualTableLayout.getDiscretePredicates().isPresent(), expectedTableLayout.getDiscretePredicates().isPresent());
        actualTableLayout.getDiscretePredicates().ifPresent(actual -> {
            DiscretePredicates expected = expectedTableLayout.getDiscretePredicates().get();
            assertEquals(actual.getColumns(), expected.getColumns());
            assertEqualsIgnoreOrder(actual.getPredicates(), expected.getPredicates());
        });
        assertEquals(actualTableLayout.getStreamPartitioningColumns(), expectedTableLayout.getStreamPartitioningColumns());
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
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            assertEquals(getAllPartitions(getOnlyElement(tableLayoutResults).getTableLayout().getHandle()).size(), 1);
            assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), unpartitionedTableLayout);
        }
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.getTableLayouts(newSession(), invalidTableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        }
    }

    @SuppressWarnings({"ValueOfIncrementOrDecrementUsed", "UnusedAssignment"})
    @Test
    public void testGetTableSchemaPartitionFormat()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), getTableHandle(metadata, tablePartitionFormat));
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
            assertPrimitiveField(map, "t_tinyint", TINYINT, false);
            assertPrimitiveField(map, "t_smallint", SMALLINT, false);
            assertPrimitiveField(map, "t_int", INTEGER, false);
            assertPrimitiveField(map, "t_bigint", BIGINT, false);
            assertPrimitiveField(map, "t_float", REAL, false);
            assertPrimitiveField(map, "t_double", DOUBLE, false);
            assertPrimitiveField(map, "t_boolean", BOOLEAN, false);
            assertPrimitiveField(map, "ds", createUnboundedVarcharType(), true);
            assertPrimitiveField(map, "file_format", createUnboundedVarcharType(), true);
            assertPrimitiveField(map, "dummy", INTEGER, true);
        }
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
            assertPrimitiveField(map, "t_tinyint", TINYINT, false);
        }
    }

    @Test
    public void testGetTableSchemaOffline()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOffline);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
        }
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOfflinePartition);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
        }
    }

    @Test
    public void testGetTableSchemaException()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertNull(metadata.getTableHandle(newSession(), invalidTable));
        }
    }

    @Test
    public void testGetPartitionSplitsBatch()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

            assertEquals(getSplitCount(splitSource), partitionCount);
        }
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

            assertEquals(getSplitCount(splitSource), 1);
        }
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            splitManager.getSplits(transaction.getTransactionHandle(), newSession(), invalidTableLayoutHandle);
        }
    }

    @Test
    public void testGetPartitionSplitsEmpty()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), newSession(), emptyTableLayoutHandle);
            // fetch full list
            getSplitCount(splitSource);
        }
    }

    @Test
    public void testGetPartitionTableOffline()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOffline);
            try {
                metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
                fail("expected TableOfflineException");
            }
            catch (TableOfflineException e) {
                assertEquals(e.getTableName(), tableOffline);
            }
        }
    }

    @Test
    public void testGetPartitionSplitsTableOfflinePartition()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOfflinePartition);
            assertNotNull(tableHandle);

            ColumnHandle dsColumn = metadata.getColumnHandles(session, tableHandle).get("ds");
            assertNotNull(dsColumn);

            Domain domain = Domain.singleValue(createUnboundedVarcharType(), utf8Slice("2012-12-30"));
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(dsColumn, domain));
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
            try {
                getSplitCount(splitManager.getSplits(transaction.getTransactionHandle(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
                fail("Expected PartitionOfflineException");
            }
            catch (PartitionOfflineException e) {
                assertEquals(e.getTableName(), tableOfflinePartition);
                assertEquals(e.getPartition(), "ds=2012-12-30");
            }
        }
    }

    @Test
    public void testBucketedTableStringInt()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedStringInt);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            assertTableIsBucketed(tableHandle);

            String testString = "test";
            Integer testInt = 13;
            Short testSmallint = 12;

            // Reverse the order of bindings as compared to bucketing order
            ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                    .put(columnHandles.get(columnIndex.get("t_int")), NullableValue.of(INTEGER, (long) testInt))
                    .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(createUnboundedVarcharType(), utf8Slice(testString)))
                    .put(columnHandles.get(columnIndex.get("t_smallint")), NullableValue.of(SMALLINT, (long) testSmallint))
                    .build();

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

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
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testBucketedTableBigintBoolean()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedBigintBoolean);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            assertTableIsBucketed(tableHandle);

            String testString = "test";
            Long testBigint = 89L;
            Boolean testBoolean = true;

            ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                    .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(createUnboundedVarcharType(), utf8Slice(testString)))
                    .put(columnHandles.get(columnIndex.get("t_bigint")), NullableValue.of(BIGINT, testBigint))
                    .put(columnHandles.get(columnIndex.get("t_boolean")), NullableValue.of(BOOLEAN, testBoolean))
                    .build();

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

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
    }

    @Test
    public void testBucketedTableDoubleFloat()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedDoubleFloat);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            assertTableIsBucketed(tableHandle);

            ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                    .put(columnHandles.get(columnIndex.get("t_float")), NullableValue.of(REAL, (long) floatToRawIntBits(87.1f)))
                    .put(columnHandles.get(columnIndex.get("t_double")), NullableValue.of(DOUBLE, 88.2))
                    .build();

            // floats and doubles are not supported, so we should see all splits
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(32), Optional.empty());
            assertEquals(result.getRowCount(), 100);
        }
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
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
            assertEquals(splits.size(), partitionCount);
            for (ConnectorSplit split : splits) {
                HiveSplit hiveSplit = (HiveSplit) split;

                List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
                String ds = partitionKeys.get(0).getValue();
                String fileFormat = partitionKeys.get(1).getValue();
                HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase());
                int dummyPartition = Integer.parseInt(partitionKeys.get(2).getValue());

                long rowNumber = 0;
                long completedBytes = 0;
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, columnHandles)) {
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
                        Object value;

                        value = row.getField(columnIndex.get("t_string"));
                        if (rowNumber % 19 == 0) {
                            assertNull(value);
                        }
                        else if (rowNumber % 19 == 1) {
                            assertEquals(value, "");
                        }
                        else {
                            assertEquals(value, "test");
                        }

                        assertEquals(row.getField(columnIndex.get("t_tinyint")), (byte) (1 + rowNumber));
                        assertEquals(row.getField(columnIndex.get("t_smallint")), (short) (2 + rowNumber));
                        assertEquals(row.getField(columnIndex.get("t_int")), 3 + (int) rowNumber);

                        if (rowNumber % 13 == 0) {
                            assertNull(row.getField(columnIndex.get("t_bigint")));
                        }
                        else {
                            assertEquals(row.getField(columnIndex.get("t_bigint")), 4 + rowNumber);
                        }

                        assertEquals((Float) row.getField(columnIndex.get("t_float")), 5.1f + rowNumber, 0.001);
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
    }

    @Test
    public void testGetPartialRecords()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
            assertEquals(splits.size(), partitionCount);
            for (ConnectorSplit split : splits) {
                HiveSplit hiveSplit = (HiveSplit) split;

                List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
                String ds = partitionKeys.get(0).getValue();
                String fileFormat = partitionKeys.get(1).getValue();
                HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase());
                int dummyPartition = Integer.parseInt(partitionKeys.get(2).getValue());

                long rowNumber = 0;
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, columnHandles)) {
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
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
            Map<String, Integer> columnIndex = indexColumns(columnHandles);

            List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
            assertEquals(splits.size(), 1);

            for (ConnectorSplit split : splits) {
                HiveSplit hiveSplit = (HiveSplit) split;

                assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

                long rowNumber = 0;
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, columnHandles)) {
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

                        assertEquals(row.getField(columnIndex.get("t_tinyint")), (byte) (1 + rowNumber));
                    }
                }
                assertEquals(rowNumber, 100);
            }
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetRecordsInvalidColumn()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata connectorMetadata = transaction.getMetadata();
            ConnectorTableHandle table = getTableHandle(connectorMetadata, tableUnpartitioned);
            readTable(transaction, table, ImmutableList.of(invalidColumnHandle), newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*The column 't_data' in table '.*\\.presto_test_partition_schema_change' is declared as type 'bigint', but partition 'ds=2012-12-29' declared column 't_data' as type 'string'.")
    public void testPartitionSchemaMismatch()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle table = getTableHandle(metadata, tablePartitionSchemaChange);
            readTable(transaction, table, ImmutableList.of(dsColumn), newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
        }
    }

    // TODO coercion of non-canonical values should be supported
    @Test(enabled = false)
    public void testPartitionSchemaNonCanonical()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            ConnectorTableHandle table = getTableHandle(metadata, tablePartitionSchemaChangeNonCanonical);
            ColumnHandle column = metadata.getColumnHandles(session, table).get("t_boolean");
            assertNotNull(column);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, table, new Constraint<>(TupleDomain.fromFixedValues(ImmutableMap.of(column, NullableValue.of(BOOLEAN, false))), bindings -> true), Optional.empty());
            ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
            assertEquals(getAllPartitions(layoutHandle).size(), 1);
            assertEquals(getPartitionId(getAllPartitions(layoutHandle).get(0)), "t_boolean=0");

            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle);
            ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

            ImmutableList<ColumnHandle> columnHandles = ImmutableList.of(column);
            try (ConnectorPageSource ignored = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, columnHandles)) {
                fail("expected exception");
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), HIVE_INVALID_PARTITION_VALUE.toErrorCode());
            }
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
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            if (metadata.getTableHandle(session, new SchemaTableName(database, "presto_test_types_rctext")) == null) {
                return;
            }

            ConnectorTableHandle tableHandle = getTableHandle(metadata, new SchemaTableName(database, "presto_test_types_rctext"));
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            HiveSplit hiveSplit = getHiveSplit(tableHandle);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                    new HiveClientConfig().setTimeZone(timeZone.getID()),
                    hdfsEnvironment,
                    ImmutableSet.of(new ColumnarTextHiveRecordCursorProvider(hdfsEnvironment)),
                    ImmutableSet.<HivePageSourceFactory>of(),
                    TYPE_MANAGER);

            ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, columnHandles);
            assertGetRecords(RCTEXT, tableMetadata, hiveSplit, pageSource, columnHandles);
        }
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
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            if (metadata.getTableHandle(session, new SchemaTableName(database, "presto_test_types_rcbinary")) == null) {
                return;
            }

            ConnectorTableHandle tableHandle = getTableHandle(metadata, new SchemaTableName(database, "presto_test_types_rcbinary"));
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            HiveSplit hiveSplit = getHiveSplit(tableHandle);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                    new HiveClientConfig().setTimeZone(timeZone.getID()),
                    hdfsEnvironment,
                    ImmutableSet.of(new ColumnarBinaryHiveRecordCursorProvider(hdfsEnvironment)),
                    ImmutableSet.<HivePageSourceFactory>of(),
                    TYPE_MANAGER);

            ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, columnHandles);
            assertGetRecords(RCBINARY, tableMetadata, hiveSplit, pageSource, columnHandles);
        }
    }

    @Test
    public void testTypesOrc()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_orc", ORC);
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
    public void testHiveViewsAreNotSupported()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            try {
                ConnectorMetadata metadata = transaction.getMetadata();
                getTableHandle(metadata, view);
                fail("Expected HiveViewNotSupportedException");
            }
            catch (HiveViewNotSupportedException e) {
                assertEquals(e.getTableName(), view);
            }
        }
    }

    @Test
    public void testHiveViewsHaveNoColumns()
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertEquals(metadata.listTableColumns(newSession(), new SchemaTablePrefix(view.getSchemaName(), view.getTableName())), ImmutableMap.of());
        }
    }

    @Test
    public void testRenameTable()
    {
        try {
            createDummyTable(temporaryRenameTableOld);

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                metadata.renameTable(session, getTableHandle(metadata, temporaryRenameTableOld), temporaryRenameTableNew);
                transaction.commit();
            }

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                assertNull(metadata.getTableHandle(session, temporaryRenameTableOld));
                assertNotNull(metadata.getTableHandle(session, temporaryRenameTableNew));
            }
        }
        finally {
            dropTable(temporaryRenameTableOld);
            dropTable(temporaryRenameTableNew);
        }
    }

    @Test
    public void testTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doCreateTable(temporaryCreateTable, storageFormat);
            }
            finally {
                dropTable(temporaryCreateTable);
            }
        }
    }

    @Test
    public void testTableCreationRollback()
            throws Exception
    {
        try {
            Path stagingPathRoot;
            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                // begin creating the table
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateRollbackTable, CREATE_TABLE_COLUMNS, createTableProperties(RCBINARY));

                ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());

                // write the data
                ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle);
                sink.appendPage(CREATE_TABLE_DATA.toPage());
                getFutureValue(sink.finish());

                // verify we have data files
                stagingPathRoot = getStagingPathRoot(outputHandle);
                assertFalse(listAllDataFiles(stagingPathRoot).isEmpty());

                // rollback the table
                transaction.rollback();
            }

            // verify all files have been deleted
            assertTrue(listAllDataFiles(stagingPathRoot).isEmpty());

            // verify table is not in the metastore
            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                assertNull(metadata.getTableHandle(session, temporaryCreateRollbackTable));
            }
        }
        finally {
            dropTable(temporaryCreateRollbackTable);
        }
    }

    @Test
    public void testInsert()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doInsert(storageFormat, temporaryInsertTable);
            }
            finally {
                dropTable(temporaryInsertTable);
            }
        }
    }

    @Test
    public void testInsertIntoNewPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doInsertIntoNewPartition(storageFormat, temporaryInsertIntoNewPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoNewPartitionTable);
            }
        }
    }

    @Test
    public void testInsertIntoExistingPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doInsertIntoExistingPartition(storageFormat, temporaryInsertIntoExistingPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoExistingPartitionTable);
            }
        }
    }

    @Test
    public void testInsertUnsupportedWriteType()
            throws Exception
    {
        try {
            doInsertUnsupportedWriteType(ORC, temporaryInsertUnsupportedWriteType);
        }
        finally {
            dropTable(temporaryInsertUnsupportedWriteType);
        }
    }

    @Test
    public void testMetadataDelete()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doTestMetadataDelete(storageFormat, temporaryMetadataDeleteTable);
            }
            finally {
                dropTable(temporaryMetadataDeleteTable);
            }
        }
    }

    @Test
    public void testEmptyTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doCreateEmptyTable(temporaryCreateEmptyTable, storageFormat, CREATE_TABLE_COLUMNS);
            }
            finally {
                dropTable(temporaryCreateEmptyTable);
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
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.dropView(newSession(), temporaryCreateView);
            }
            catch (RuntimeException e) {
                // this usually occurs because the view was not created
            }
        }
    }

    @Test
    public void testCreateTableUnsupportedType()
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", HYPER_LOG_LOG));
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(invalidTable, columns, createTableProperties(storageFormat));
                metadata.beginCreateTable(session, tableMetadata, Optional.empty());
                fail("create table with unsupported type should fail for storage format " + storageFormat);
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            }
        }
    }

    private void createDummyTable(SchemaTableName tableName)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", createUnboundedVarcharType()));
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(TEXTFILE));
            ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
            metadata.finishCreateTable(session, handle, ImmutableList.of());

            transaction.commit();
        }
    }

    private void verifyViewCreation()
    {
        // replace works for new view
        doCreateView(temporaryCreateView, true);

        // replace works for existing view
        doCreateView(temporaryCreateView, true);

        // create fails for existing view
        try {
            doCreateView(temporaryCreateView, false);
            fail("create existing should fail");
        }
        catch (ViewAlreadyExistsException e) {
            assertEquals(e.getViewName(), temporaryCreateView);
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            // drop works when view exists
            metadata.dropView(newSession(), temporaryCreateView);
            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertEquals(metadata.getViews(newSession(), temporaryCreateView.toSchemaTablePrefix()).size(), 0);
            assertFalse(metadata.listViews(newSession(), temporaryCreateView.getSchemaName()).contains(temporaryCreateView));
        }

        // drop fails when view does not exist
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.dropView(newSession(), temporaryCreateView);
            fail("drop non-existing should fail");
        }
        catch (ViewNotFoundException e) {
            assertEquals(e.getViewName(), temporaryCreateView);
        }

        // create works for new view
        doCreateView(temporaryCreateView, false);
    }

    private void doCreateView(SchemaTableName viewName, boolean replace)
    {
        String viewData = "test data";
        try (Transaction transaction = newTransaction()) {
            transaction.getMetadata().createView(newSession(), viewName, viewData, replace);
            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(newSession(), viewName.toSchemaTablePrefix());
            assertEquals(views.size(), 1);
            assertEquals(views.get(viewName).getViewData(), viewData);

            assertTrue(metadata.listViews(newSession(), viewName.getSchemaName()).contains(viewName));
        }
    }

    protected void doCreateTable(SchemaTableName tableName, HiveStorageFormat storageFormat)
            throws Exception
    {
        String queryId;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            queryId = session.getQueryId();

            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, CREATE_TABLE_COLUMNS, createTableProperties(storageFormat));

            ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());

            // write the data
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            // verify all new files start with the unique prefix
            for (String filePath : listAllDataFiles(getStagingPathRoot(outputHandle))) {
                assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(outputHandle)));
            }

            // commit the table
            metadata.finishCreateTable(session, outputHandle, fragments);

            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            // load the new table
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
            assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), CREATE_TABLE_COLUMNS);

            // verify the data
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_DATA.getMaterializedRows());

            // verify the node version and query ID in table
            Table table = getMetastoreClient(tableName.getSchemaName()).getTable(tableName.getSchemaName(), tableName.getTableName()).get();
            assertEquals(table.getParameters().get(HiveMetadata.PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
            assertEquals(table.getParameters().get(HiveMetadata.PRESTO_QUERY_ID_NAME), queryId);
        }
    }

    protected void doCreateEmptyTable(SchemaTableName tableName, HiveStorageFormat storageFormat, List<ColumnMetadata> createTableColumns)
            throws Exception
    {
        List<String> partitionedBy = createTableColumns.stream()
                .filter(column -> column.getName().equals("ds"))
                .map(ColumnMetadata::getName)
                .collect(toList());

        String queryId;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            queryId = session.getQueryId();

            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, createTableColumns, createTableProperties(storageFormat, partitionedBy));
            metadata.createTable(session, tableMetadata);
            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            // load the new table
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));

            List<ColumnMetadata> expectedColumns = createTableColumns.stream()
                    .map(column -> new ColumnMetadata(
                            column.getName(),
                            column.getType(),
                            annotateColumnComment(Optional.ofNullable(column.getComment()), partitionedBy.contains(column.getName())),
                            false))
                    .collect(toList());
            assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), expectedColumns);

            // verify table format
            Table table = transaction.getMetastore(tableName.getSchemaName()).getTable(tableName.getSchemaName(), tableName.getTableName()).get();
            assertEquals(table.getStorage().getStorageFormat().getInputFormat(), storageFormat.getInputFormat());

            // verify the node version and query ID
            assertEquals(table.getParameters().get(HiveMetadata.PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
            assertEquals(table.getParameters().get(HiveMetadata.PRESTO_QUERY_ID_NAME), queryId);

            // verify the table is empty
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEquals(result.getRowCount(), 0);
        }
    }

    private void doInsert(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            insertData(tableName, CREATE_TABLE_DATA);

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                // load the new table
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
                // verify the metadata
                ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
                assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), CREATE_TABLE_COLUMNS);

                // verify the data
                resultBuilder.rows(CREATE_TABLE_DATA.getMaterializedRows());
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
                assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
            }
        }

        // test rollback
        Set<String> existingFiles;
        try (Transaction transaction = newTransaction()) {
            existingFiles = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(existingFiles.isEmpty());
        }

        Path stagingPathRoot;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            getFutureValue(sink.finish());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify all temp files start with the unique prefix
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            Set<String> tempFiles = listAllDataFiles(stagingPathRoot);
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
            }

            // rollback insert
            transaction.rollback();
        }

        // verify temp directory is empty
        assertTrue(listAllDataFiles(stagingPathRoot).isEmpty());

        // verify the data is unchanged
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);
        }
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

    protected Path getStagingPathRoot(ConnectorInsertTableHandle insertTableHandle)
    {
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle;
        return getLocationService(hiveInsertTableHandle.getSchemaName()).writePathRoot(hiveInsertTableHandle.getLocationHandle()).get();
    }

    protected Path getStagingPathRoot(ConnectorOutputTableHandle outputTableHandle)
    {
        HiveOutputTableHandle hiveOutputTableHandle = (HiveOutputTableHandle) outputTableHandle;
        return getLocationService(hiveOutputTableHandle.getSchemaName()).writePathRoot(hiveOutputTableHandle.getLocationHandle()).get();
    }

    protected Path getTargetPathRoot(ConnectorInsertTableHandle insertTableHandle)
    {
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle;
        return getLocationService(hiveInsertTableHandle.getSchemaName()).targetPathRoot(hiveInsertTableHandle.getLocationHandle());
    }

    protected Set<String> listAllDataFiles(Transaction transaction, String schemaName, String tableName)
            throws IOException
    {
        Set<String> existingFiles = new HashSet<>();
        for (String location : listAllDataPaths(transaction.getMetastore(schemaName), schemaName, tableName)) {
            existingFiles.addAll(listAllDataFiles(new Path(location)));
        }
        return existingFiles;
    }

    public static List<String> listAllDataPaths(SemiTransactionalHiveMetastore metastore, String schemaName, String tableName)
    {
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        Table table = metastore.getTable(schemaName, tableName).get();
        if (table.getStorage().getLocation() != null) {
            // For partitioned table, there should be nothing directly under this directory.
            // But including this location in the set makes the directory content assert more
            // extensive, which is desirable.
            locations.add(table.getStorage().getLocation());
        }

        Optional<List<String>> partitionNames = metastore.getPartitionNames(schemaName, tableName);
        if (partitionNames.isPresent()) {
            metastore.getPartitionsByNames(schemaName, tableName, partitionNames.get()).values().stream()
                    .map(Optional::get)
                    .map(partition -> partition.getStorage().getLocation())
                    .filter(location -> !location.startsWith(table.getStorage().getLocation()))
                    .forEach(locations::add);
        }

        return locations.build();
    }

    protected Set<String> listAllDataFiles(Path path)
            throws IOException
    {
        Set<String> result = new HashSet<>();
        FileSystem fileSystem = hdfsEnvironment.getFileSystem("user", path);
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

        // insert the data
        String queryId = insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        Set<String> existingFiles;
        try (Transaction transaction = newTransaction()) {
            // verify partitions were created
            List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toList()));

            // verify the node versions in partitions
            Map<String, Optional<Partition>> partitions = getMetastoreClient(tableName.getSchemaName()).getPartitionsByNames(tableName.getSchemaName(), tableName.getTableName(), partitionNames);
            assertEquals(partitions.size(), partitionNames.size());
            for (String partitionName : partitionNames) {
                Partition partition = partitions.get(partitionName).get();
                assertEquals(partition.getParameters().get(HiveMetadata.PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
                assertEquals(partition.getParameters().get(HiveMetadata.PRESTO_QUERY_ID_NAME), queryId);
            }

            // load the new table
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

            // test rollback
            existingFiles = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(existingFiles.isEmpty());
        }

        Path stagingPathRoot;
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA_2ND.toPage());
            getFutureValue(sink.finish());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify all temp files start with the unique prefix
            Set<String> tempFiles = listAllDataFiles(getStagingPathRoot(insertTableHandle));
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
            }

            // rollback insert
            transaction.rollback();
        }

        // verify the data is unchanged
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify temp directory is empty
            assertTrue(listAllDataFiles(stagingPathRoot).isEmpty());
        }
    }

    private void doInsertUnsupportedWriteType(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        List<Column> columns = ImmutableList.of(new Column("dummy", HiveType.valueOf("uniontype<smallint,tinyint>"), Optional.empty()));
        List<Column> partitionColumns = ImmutableList.of(new Column("name", HIVE_STRING, Optional.empty()));

        createEmptyTable(tableName, storageFormat, columns, partitionColumns);

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            metadata.beginInsert(session, tableHandle);
            fail("expected failure");
        }
        catch (PrestoException e) {
            String expected = "Inserting into Hive table .* with column type uniontype<smallint,tinyint> not supported";
            if (!e.getMessage().matches(expected)) {
                throw new TestException("The exception was thrown with the wrong message:" +
                        " expected \"" + expected + "\"" + " but got \"" + e.getMessage() + "\"", e);
            }
        }
    }

    private void doInsertIntoExistingPartition(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            // insert the data
            insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

                // verify partitions were created
                List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                        .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
                assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                        .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                        .collect(toList()));

                // load the new table
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

                // verify the data
                resultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
                assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
            }
        }

        // test rollback
        Set<String> existingFiles;
        Path stagingPathRoot;
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            existingFiles = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(existingFiles.isEmpty());

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage());
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage());
            getFutureValue(sink.finish());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify all temp files start with the unique prefix
            Set<String> tempFiles = listAllDataFiles(getStagingPathRoot(insertTableHandle));
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
            }

            // rollback insert
            transaction.rollback();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data is unchanged
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify temp directory is empty
            assertTrue(listAllDataFiles(stagingPathRoot).isEmpty());
        }
    }

    /**
     * @return query id
     */
    private String insertData(SchemaTableName tableName, MaterializedResult data)
            throws Exception
    {
        Path writePath;
        Path targetPath;
        String queryId;
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            queryId = session.getQueryId();
            writePath = getStagingPathRoot(insertTableHandle);
            targetPath = getTargetPathRoot(insertTableHandle);

            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);

            // write data
            sink.appendPage(data.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            // commit the insert
            metadata.finishInsert(session, insertTableHandle, fragments);
            transaction.commit();
        }

        // check that temporary files are removed
        if (!writePath.equals(targetPath)) {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem("user", writePath);
            assertFalse(fileSystem.exists(writePath));
        }

        return queryId;
    }

    private void doTestMetadataDelete(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        MaterializedResult.Builder expectedResultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        expectedResultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            // verify partitions were created
            List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toList()));

            // verify table directory is not empty
            Set<String> filesAfterInsert = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertFalse(filesAfterInsert.isEmpty());

            // verify the data
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), expectedResultBuilder.build().getMaterializedRows());
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            // get ds column handle
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("ds");

            // delete ds=2015-07-03
            session = newSession();
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumnHandle, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2015-07-03"))));
            Constraint<ColumnHandle> constraint = new Constraint<>(tupleDomain, convertToPredicate(tupleDomain));
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, constraint, Optional.empty());
            ConnectorTableLayoutHandle tableLayoutHandle = Iterables.getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
            metadata.metadataDelete(session, tableHandle, tableLayoutHandle);

            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("ds");
            int dsColumnOrdinalPosition = columnHandles.indexOf(dsColumnHandle);

            // verify the data
            session = newSession();
            ImmutableList<MaterializedRow> expectedRows = expectedResultBuilder.build().getMaterializedRows().stream()
                    .filter(row -> !"2015-07-03".equals(row.getField(dsColumnOrdinalPosition)))
                    .collect(ImmutableCollectors.toImmutableList());
            MaterializedResult actualAfterDelete = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(actualAfterDelete.getMaterializedRows(), expectedRows);
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("ds");

            // delete ds=2015-07-01 and 2015-07-02
            session = newSession();
            TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                    ImmutableMap.of(dsColumnHandle, Domain.create(ValueSet.ofRanges(Range.range(createUnboundedVarcharType(), utf8Slice("2015-07-01"), true, utf8Slice("2015-07-02"), true)), false)));
            Constraint<ColumnHandle> constraint2 = new Constraint<>(tupleDomain2, convertToPredicate(tupleDomain2));
            List<ConnectorTableLayoutResult> tableLayoutResults2 = metadata.getTableLayouts(session, tableHandle, constraint2, Optional.empty());
            ConnectorTableLayoutHandle tableLayoutHandle2 = Iterables.getOnlyElement(tableLayoutResults2).getTableLayout().getHandle();
            metadata.metadataDelete(session, tableHandle, tableLayoutHandle2);

            transaction.commit();
        }

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data
            session = newSession();
            MaterializedResult actualAfterDelete2 = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(actualAfterDelete2.getMaterializedRows(), ImmutableList.of());

            // verify table directory is empty
            Set<String> filesAfterDelete = listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName());
            assertTrue(filesAfterDelete.isEmpty());
        }
    }

    protected void assertGetRecordsOptional(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            if (metadata.getTableHandle(newSession(), new SchemaTableName(database, tableName)) != null) {
                assertGetRecords(tableName, hiveStorageFormat);
            }
        }
    }

    protected void assertGetRecords(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, new SchemaTableName(database, tableName));
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            HiveSplit hiveSplit = getHiveSplit(tableHandle);

            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, hiveSplit, columnHandles);
            assertGetRecords(hiveStorageFormat, tableMetadata, hiveSplit, pageSource, columnHandles);
        }
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
                Object value;

                // STRING
                index = columnIndex.get("t_string");
                value = row.getField(index);
                if (rowNumber % 19 == 0) {
                    assertNull(value);
                }
                else if (rowNumber % 19 == 1) {
                    assertEquals(value, "");
                }
                else {
                    assertEquals(value, "test");
                }

                // NUMBERS
                assertEquals(row.getField(columnIndex.get("t_tinyint")), (byte) (1 + rowNumber));
                assertEquals(row.getField(columnIndex.get("t_smallint")), (short) (2 + rowNumber));
                assertEquals(row.getField(columnIndex.get("t_int")), (int) (3 + rowNumber));

                index = columnIndex.get("t_bigint");
                if ((rowNumber % 13) == 0) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), 4 + rowNumber);
                }

                assertEquals((Float) row.getField(columnIndex.get("t_float")), 5.1f + rowNumber, 0.001);
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
                        SqlTimestamp expected = new SqlTimestamp(new DateTime(2011, 5, 6, 7, 8, 9, 123, timeZone).getMillis(), UTC_KEY);
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

                // VARCHAR(50)
                index = columnIndex.get("t_varchar");
                if (index != null) {
                    value = row.getField(index);
                    if (rowNumber % 39 == 0) {
                        assertNull(value);
                    }
                    else if (rowNumber % 39 == 1) {
                        // https://issues.apache.org/jira/browse/HIVE-13289
                        // RCBINARY reads empty VARCHAR as null
                        if (hiveStorageFormat == RCBINARY) {
                            assertNull(value);
                        }
                        else {
                            assertEquals(value, "");
                        }
                    }
                    else {
                        assertEquals(value, "test varchar");
                    }
                }

                //CHAR(25)
                index = columnIndex.get("t_char");
                if (index != null) {
                    value = row.getField(index);
                    if ((rowNumber % 41) == 0) {
                        assertNull(value);
                    }
                    else {
                        assertEquals(value, (rowNumber % 41) == 1 ? "                         " : "test char                ");
                    }
                }

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
                        assertEquals(row.getField(index), ImmutableMap.of(1, ImmutableList.of(expected1, expected2)));
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
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle handle = metadata.getTableHandle(session, table);
            if (handle == null) {
                return;
            }

            metadata.dropTable(session, handle);
            try {
                // todo I have no idea why this is needed... maybe there is a propagation delay in the metastore?
                metadata.dropTable(session, handle);
                fail("expected NotFoundException");
            }
            catch (TableNotFoundException expected) {
            }

            transaction.commit();
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    protected ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(newSession(), tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private MaterializedResult readTable(
            Transaction transaction,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            ConnectorSession session,
            TupleDomain<ColumnHandle> tupleDomain,
            OptionalInt expectedSplitCount,
            Optional<HiveStorageFormat> expectedStorageFormat)
            throws Exception
    {
        List<ConnectorTableLayoutResult> tableLayoutResults = transaction.getMetadata().getTableLayouts(
                session,
                tableHandle,
                new Constraint<>(tupleDomain, bindings -> true),
                Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle));
        if (expectedSplitCount.isPresent()) {
            assertEquals(splits.size(), expectedSplitCount.getAsInt());
        }

        ImmutableList.Builder<MaterializedRow> allRows = ImmutableList.builder();
        for (ConnectorSplit split : splits) {
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, columnHandles)) {
                if (expectedStorageFormat.isPresent()) {
                    assertPageSourceType(pageSource, expectedStorageFormat.get());
                }
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                allRows.addAll(result.getMaterializedRows());
            }
        }
        return new MaterializedResult(allRows.build(), getTypes(columnHandles));
    }

    public ExtendedHiveMetastore getMetastoreClient(String namespace)
    {
        return metastoreClient;
    }

    public LocationService getLocationService(String namespace)
    {
        return locationService;
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
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
            ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
            return getAllSplits(splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle));
        }
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
            HiveRecordCursor hiveRecordCursor = (HiveRecordCursor) ((RecordPageSource) pageSource).getCursor();
            assertInstanceOf(hiveRecordCursor.getRegularColumnRecordCursor(), recordCursorType(hiveStorageFormat), hiveStorageFormat.name());
        }
        else {
            assertInstanceOf(((HivePageSource) pageSource).getPageSource(), pageSourceType(hiveStorageFormat), hiveStorageFormat.name());
        }
    }

    private static Class<? extends RecordCursor> recordCursorType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
            case RCTEXT:
                return ColumnarTextHiveRecordCursor.class;
            case RCBINARY:
                return ColumnarBinaryHiveRecordCursor.class;
            case PARQUET:
                return ParquetHiveRecordCursor.class;
        }
        return GenericHiveRecordCursor.class;
    }

    private static Class<? extends ConnectorPageSource> pageSourceType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
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
                else if (TINYINT.equals(column.getType())) {
                    assertInstanceOf(value, Byte.class);
                }
                else if (SMALLINT.equals(column.getType())) {
                    assertInstanceOf(value, Short.class);
                }
                else if (INTEGER.equals(column.getType())) {
                    assertInstanceOf(value, Integer.class);
                }
                else if (BIGINT.equals(column.getType())) {
                    assertInstanceOf(value, Long.class);
                }
                else if (DOUBLE.equals(column.getType())) {
                    assertInstanceOf(value, Double.class);
                }
                else if (REAL.equals(column.getType())) {
                    assertInstanceOf(value, Float.class);
                }
                else if (isVarcharType(column.getType())) {
                    assertInstanceOf(value, String.class);
                }
                else if (isCharType(column.getType())) {
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
        assertEquals(column.getComment(), annotateColumnComment(Optional.empty(), partitionKey));
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

    protected static String randomName()
    {
        return UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
    }

    protected static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat)
    {
        return createTableProperties(storageFormat, ImmutableList.of());
    }

    private static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat, Iterable<String> parititonedBy)
    {
        return ImmutableMap.<String, Object>builder()
                .put(STORAGE_FORMAT_PROPERTY, storageFormat)
                .put(PARTITIONED_BY_PROPERTY, ImmutableList.copyOf(parititonedBy))
                .put(BUCKETED_BY_PROPERTY, ImmutableList.of())
                .put(BUCKET_COUNT_PROPERTY, 0)
                .build();
    }

    protected static List<ColumnHandle> filterNonHiddenColumnHandles(Collection<ColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                .collect(toList());
    }

    protected static List<ColumnMetadata> filterNonHiddenColumnMetadata(Collection<ColumnMetadata> columnMetadatas)
    {
        return columnMetadatas.stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(toList());
    }

    protected void createEmptyTable(SchemaTableName schemaTableName, HiveStorageFormat hiveStorageFormat, List<Column> columns, List<Column> partitionColumns)
            throws Exception
    {
        Path targetPath;

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();

            String tableOwner = session.getUser();
            String schemaName = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();

            LocationService locationService = getLocationService(schemaName);
            LocationHandle locationHandle = locationService.forNewTable(transaction.getMetastore(schemaName), session.getUser(), session.getQueryId(), schemaName, tableName);
            targetPath = locationService.targetPathRoot(locationHandle);

            Table.Builder tableBuilder = Table.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setOwner(tableOwner)
                    .setTableType(TableType.MANAGED_TABLE.name())
                    .setParameters(ImmutableMap.of())
                    .setDataColumns(columns)
                    .setPartitionColumns(partitionColumns);

            tableBuilder.getStorageBuilder()
                    .setLocation(targetPath.toString())
                    .setStorageFormat(StorageFormat.create(hiveStorageFormat.getSerDe(), hiveStorageFormat.getInputFormat(), hiveStorageFormat.getOutputFormat()))
                    .setSerdeParameters(ImmutableMap.of());

            PrivilegeGrantInfo allPrivileges = new PrivilegeGrantInfo("all", 0, tableOwner, PrincipalType.USER, true);
            PrincipalPrivilegeSet principalPrivilegeSet = new PrincipalPrivilegeSet(
                    ImmutableMap.of(session.getUser(), ImmutableList.of(allPrivileges)),
                    ImmutableMap.of(),
                    ImmutableMap.of());
            transaction.getMetastore(schemaName).createTable(session, tableBuilder.build(), principalPrivilegeSet, Optional.empty());

            transaction.commit();
        }

        ConnectorSession session = newSession();
        List<String> targetDirectoryList = listDirectory(session.getUser(), targetPath);
        assertEquals(targetDirectoryList, ImmutableList.of());
    }

    private List<String> listDirectory(String user, Path path)
            throws IOException
    {
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path);
        ImmutableList.Builder<String> result = ImmutableList.builder();
        for (FileStatus fileStatus : fileSystem.listStatus(path)) {
            result.add(fileStatus.getPath().getName());
        }
        return result.build();
    }

    @Test
    public void testTransactionDeleteInsert()
            throws Exception
    {
        try {
            doTestTransactionDeleteInsert(
                    RCBINARY,
                    temporaryDeleteInsert,
                    true,
                    ImmutableList.of(
                            new TransactionDeleteInsertTestCase(false, false, ROLLBACK_RIGHT_AWAY, Optional.empty()),
                            new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_DELETE, Optional.empty()),
                            new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_BEGIN_INSERT, Optional.empty()),
                            new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_APPEND_PAGE, Optional.empty()),
                            new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_SINK_FINISH, Optional.empty()),
                            new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_FINISH_INSERT, Optional.empty()),
                            new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new AddPartitionFailure())),
                            new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new DirectoryRenameFailure())),
                            new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new FileRenameFailure())),
                            new TransactionDeleteInsertTestCase(true, false, COMMIT, Optional.of(new DropPartitionFailure())),
                            new TransactionDeleteInsertTestCase(true, true, COMMIT, Optional.empty())));
        }
        finally {
            dropTable(temporaryDeleteInsert);
        }
    }

    protected void doTestTransactionDeleteInsert(HiveStorageFormat storageFormat, SchemaTableName tableName, boolean allowInsertExisting, List<TransactionDeleteInsertTestCase> testCases)
            throws Exception
    {
        // There are 4 types of operations on a partition: add, drop, alter (drop then add), insert existing.
        // There are 12 partitions in this test, 3 for each type.
        // 3 is chosen to verify that cleanups, commit aborts, rollbacks are always as complete as possible regardless of failure.
        MaterializedResult beforeData =
                MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                        .row(110L, "a", "alter1")
                        .row(120L, "a", "insert1")
                        .row(140L, "a", "drop1")
                        .row(210L, "b", "drop2")
                        .row(310L, "c", "alter2")
                        .row(320L, "c", "alter3")
                        .row(510L, "e", "drop3")
                        .row(610L, "f", "insert2")
                        .row(620L, "f", "insert3")
                        .build();
        Domain domainToDrop = Domain.create(ValueSet.of(
                createUnboundedVarcharType(),
                utf8Slice("alter1"), utf8Slice("alter2"), utf8Slice("alter3"), utf8Slice("drop1"), utf8Slice("drop2"), utf8Slice("drop3")),
                false);
        List<MaterializedRow> extraRowsForInsertExisting = ImmutableList.of();
        if (allowInsertExisting) {
            extraRowsForInsertExisting = MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                    .row(121L, "a", "insert1")
                    .row(611L, "f", "insert2")
                    .row(621L, "f", "insert3")
                    .build()
                    .getMaterializedRows();
        }
        MaterializedResult insertData =
                MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                        .row(111L, "a", "alter1")
                        .row(131L, "a", "add1")
                        .row(221L, "b", "add2")
                        .row(311L, "c", "alter2")
                        .row(321L, "c", "alter3")
                        .row(411L, "d", "add3")
                        .rows(extraRowsForInsertExisting)
                        .build();
        MaterializedResult afterData =
                MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                        .row(120L, "a", "insert1")
                        .row(610L, "f", "insert2")
                        .row(620L, "f", "insert3")
                        .rows(insertData.getMaterializedRows())
                        .build();

        boolean dirtyState = true;
        for (TransactionDeleteInsertTestCase testCase : testCases) {
            if (dirtyState) {
                // re-initialize the table
                dropTable(tableName);
                createEmptyTable(
                        tableName,
                        storageFormat,
                        ImmutableList.of(new Column("col1", HIVE_LONG, Optional.empty())),
                        ImmutableList.of(new Column("pk1", HIVE_STRING, Optional.empty()), new Column("pk2", HIVE_STRING, Optional.empty())));
                insertData(tableName, beforeData);
                dirtyState = false;
            }
            try {
                doTestTransactionDeleteInsert(
                        storageFormat,
                        tableName,
                        domainToDrop,
                        insertData,
                        testCase.isExpectCommitedData() ? afterData : beforeData,
                        testCase.getTag(),
                        testCase.isExpectQuerySucceed(),
                        testCase.getConflictTrigger());
            }
            catch (AssertionError e) {
                throw new AssertionError(format("Test case: %s", testCase.toString()), e);
            }
            if (testCase.isExpectCommitedData()) {
                dirtyState = true;
            }
        }
    }

    private void doTestTransactionDeleteInsert(
            HiveStorageFormat storageFormat,
            SchemaTableName tableName,
            Domain domainToDrop,
            MaterializedResult insertData,
            MaterializedResult expectedData,
            TransactionDeleteInsertTestTag tag,
            boolean expectQuerySucceed,
            Optional<ConflictTrigger> conflictTrigger)
            throws Exception
    {
        Path writePath = null;
        Path targetPath = null;

        try (Transaction transaction = newTransaction()) {
            try {
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                ConnectorSession session;
                rollbackIfEquals(tag, ROLLBACK_RIGHT_AWAY);

                // Query 1: delete
                session = newSession();
                HiveColumnHandle dsColumnHandle = (HiveColumnHandle) metadata.getColumnHandles(session, tableHandle).get("pk2");
                TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                        dsColumnHandle, domainToDrop));
                Constraint<ColumnHandle> constraint = new Constraint<>(tupleDomain, convertToPredicate(tupleDomain));
                List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, constraint, Optional.empty());
                ConnectorTableLayoutHandle tableLayoutHandle = Iterables.getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
                metadata.metadataDelete(session, tableHandle, tableLayoutHandle);
                rollbackIfEquals(tag, ROLLBACK_AFTER_DELETE);

                // Query 2: insert
                session = newSession();
                ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
                rollbackIfEquals(tag, ROLLBACK_AFTER_BEGIN_INSERT);
                writePath = getStagingPathRoot(insertTableHandle);
                targetPath = getTargetPathRoot(insertTableHandle);
                ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
                sink.appendPage(insertData.toPage());
                rollbackIfEquals(tag, ROLLBACK_AFTER_APPEND_PAGE);
                Collection<Slice> fragments = getFutureValue(sink.finish());
                rollbackIfEquals(tag, ROLLBACK_AFTER_SINK_FINISH);
                metadata.finishInsert(session, insertTableHandle, fragments);
                rollbackIfEquals(tag, ROLLBACK_AFTER_FINISH_INSERT);

                assertEquals(tag, COMMIT);

                if (conflictTrigger.isPresent()) {
                    JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
                    List<PartitionUpdate> partitionUpdates = fragments.stream()
                            .map(Slice::getBytes)
                            .map(partitionUpdateCodec::fromJson)
                            .collect(toList());
                    conflictTrigger.get().triggerConflict(session, tableName, insertTableHandle, partitionUpdates);
                }
                transaction.commit();
                if (conflictTrigger.isPresent()) {
                    assertTrue(expectQuerySucceed);
                    conflictTrigger.get().verifyAndCleanup(tableName);
                }
            }
            catch (TestingRollbackException e) {
                transaction.rollback();
            }
            catch (PrestoException e) {
                assertFalse(expectQuerySucceed);
                if (conflictTrigger.isPresent()) {
                    conflictTrigger.get().verifyAndCleanup(tableName);
                }
            }
        }

        // check that temporary files are removed
        if (writePath != null && !writePath.equals(targetPath)) {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem("user", writePath);
            assertFalse(fileSystem.exists(writePath));
        }

        try (Transaction transaction = newTransaction()) {
            // verify partitions
            List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName())
                    .getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
            assertEqualsIgnoreOrder(
                    partitionNames,
                    expectedData.getMaterializedRows().stream()
                            .map(row -> format("pk1=%s/pk2=%s", row.getField(1), row.getField(2)))
                            .distinct()
                            .collect(toList()));

            // load the new table
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), expectedData.getMaterializedRows());
        }
    }

    private void rollbackIfEquals(TransactionDeleteInsertTestTag tag, TransactionDeleteInsertTestTag expectedTag)
    {
        if (expectedTag.equals(tag)) {
            throw new TestingRollbackException();
        }
    }

    private static class TestingRollbackException
            extends RuntimeException
    {
    }

    protected static class TransactionDeleteInsertTestCase
    {
        private final boolean expectCommitedData;
        private final boolean expectQuerySucceed;
        private final TransactionDeleteInsertTestTag tag;
        private final Optional<ConflictTrigger> conflictTrigger;

        public TransactionDeleteInsertTestCase(boolean expectCommitedData, boolean expectQuerySucceed, TransactionDeleteInsertTestTag tag, Optional<ConflictTrigger> conflictTrigger)
        {
            this.expectCommitedData = expectCommitedData;
            this.expectQuerySucceed = expectQuerySucceed;
            this.tag = tag;
            this.conflictTrigger = conflictTrigger;
        }

        public boolean isExpectCommitedData()
        {
            return expectCommitedData;
        }

        public boolean isExpectQuerySucceed()
        {
            return expectQuerySucceed;
        }

        public TransactionDeleteInsertTestTag getTag()
        {
            return tag;
        }

        public Optional<ConflictTrigger> getConflictTrigger()
        {
            return conflictTrigger;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("tag", tag)
                    .add("conflictTrigger", conflictTrigger.map(conflictTrigger -> conflictTrigger.getClass().getName()))
                    .add("expectCommitedData", expectCommitedData)
                    .add("expectQuerySucceed", expectQuerySucceed)
                    .toString();
        }
    }

    protected enum TransactionDeleteInsertTestTag
    {
        ROLLBACK_RIGHT_AWAY,
        ROLLBACK_AFTER_DELETE,
        ROLLBACK_AFTER_BEGIN_INSERT,
        ROLLBACK_AFTER_APPEND_PAGE,
        ROLLBACK_AFTER_SINK_FINISH,
        ROLLBACK_AFTER_FINISH_INSERT,
        COMMIT,
    }

    protected interface ConflictTrigger
    {
        void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
                throws IOException;

        void verifyAndCleanup(SchemaTableName tableName)
                throws IOException;
    }

    protected class AddPartitionFailure
            implements ConflictTrigger
    {
        private final ImmutableList<String> copyPartitionFrom = ImmutableList.of("a", "insert1");
        private final ImmutableList<String> partitionValueToConflict = ImmutableList.of("b", "add2");
        private Partition conflictPartition;

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
        {
            // This method bypasses transaction interface because this method is inherently hacky and doesn't work well with the transaction abstraction.
            // Additionally, this method is not part of a test. Its purpose is to set up an environment for another test.
            ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
            Optional<Partition> partition = metastoreClient.getPartition(tableName.getSchemaName(), tableName.getTableName(), copyPartitionFrom);
            conflictPartition = Partition.builder(partition.get())
                    .setValues(partitionValueToConflict)
                    .build();
            metastoreClient.addPartitions(tableName.getSchemaName(), tableName.getTableName(), ImmutableList.of(conflictPartition));
        }

        @Override
        public void verifyAndCleanup(SchemaTableName tableName)
        {
            // This method bypasses transaction interface because this method is inherently hacky and doesn't work well with the transaction abstraction.
            // Additionally, this method is not part of a test. Its purpose is to set up an environment for another test.
            ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
            Optional<Partition> actualPartition = metastoreClient.getPartition(tableName.getSchemaName(), tableName.getTableName(), partitionValueToConflict);
            // Make sure the partition inserted to trigger conflict was not overwritten
            // Checking storage location is sufficient because implement never uses .../pk1=a/pk2=a2 as the directory for partition [b, b2].
            assertEquals(actualPartition.get().getStorage().getLocation(), conflictPartition.getStorage().getLocation());
            metastoreClient.dropPartition(tableName.getSchemaName(), tableName.getTableName(), conflictPartition.getValues(), false);
        }
    }

    protected class DropPartitionFailure
            implements ConflictTrigger
    {
        private final ImmutableList<String> partitionValueToConflict = ImmutableList.of("b", "drop2");

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
        {
            // This method bypasses transaction interface because this method is inherently hacky and doesn't work well with the transaction abstraction.
            // Additionally, this method is not part of a test. Its purpose is to set up an environment for another test.
            ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
            metastoreClient.dropPartition(tableName.getSchemaName(), tableName.getTableName(), partitionValueToConflict, false);
        }

        @Override
        public void verifyAndCleanup(SchemaTableName tableName)
        {
            // Do not add back the deleted partition because the implementation is expected to move forward instead of backward when delete fails
        }
    }

    protected class DirectoryRenameFailure
            implements ConflictTrigger
    {
        private String user;
        private Path path;

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
        {
            Path writePath = getStagingPathRoot(insertTableHandle);
            Path targetPath = getTargetPathRoot(insertTableHandle);
            if (writePath.equals(targetPath)) {
                // This conflict does not apply. Trigger a rollback right away so that this test case passes.
                throw new TestingRollbackException();
            }
            path = new Path(targetPath + "/pk1=b/pk2=add2");
            user = session.getUser();
            createDirectory(user, hdfsEnvironment, path);
        }

        @Override
        public void verifyAndCleanup(SchemaTableName tableName)
                throws IOException
        {
            assertEquals(listDirectory(user, path), ImmutableList.of());
            hdfsEnvironment.getFileSystem(user, path).delete(path, false);
        }
    }

    protected class FileRenameFailure
            implements ConflictTrigger
    {
        private String user;
        private Path path;

        @Override
        public void triggerConflict(ConnectorSession session, SchemaTableName tableName, ConnectorInsertTableHandle insertTableHandle, List<PartitionUpdate> partitionUpdates)
                throws IOException
        {
            for (PartitionUpdate partitionUpdate : partitionUpdates) {
                if ("pk2=insert2".equals(partitionUpdate.getTargetPath().getName())) {
                    path = new Path(partitionUpdate.getTargetPath(), partitionUpdate.getFileNames().get(0));
                    break;
                }
            }
            assertNotNull(path);

            user = session.getUser();
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path);
            fileSystem.createNewFile(path);
        }

        @Override
        public void verifyAndCleanup(SchemaTableName tableName)
                throws IOException
        {
            // The file we added to trigger a conflict was cleaned up because it matches the query prefix.
            // Consider this the same as a network failure that caused the successful creation of file not reported to the caller.
            assertEquals(hdfsEnvironment.getFileSystem(user, path).exists(path), false);
        }
    }
}
