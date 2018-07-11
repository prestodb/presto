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
import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.LocationService.WriteInfo;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.TestingHiveCluster;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.orc.OrcPageSource;
import com.facebook.presto.hive.parquet.ParquetHiveRecordCursor;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.hive.rcfile.RcFilePageSource;
import com.facebook.presto.metadata.MetadataManager;
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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.block.Block;
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
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.RowFieldName;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.COMMIT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_APPEND_PAGE;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_BEGIN_INSERT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_DELETE;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_FINISH_INSERT;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_AFTER_SINK_FINISH;
import static com.facebook.presto.hive.AbstractTestHiveClient.TransactionDeleteInsertTestTag.ROLLBACK_RIGHT_AWAY;
import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.bucketColumnHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static com.facebook.presto.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static com.facebook.presto.hive.HiveMetadata.convertToPredicate;
import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.JSON;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.SEQUENCEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.PAGE_SORTER;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.arrayType;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveDataStreamFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.hive.HiveTestUtils.mapType;
import static com.facebook.presto.hive.HiveTestUtils.rowType;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static com.facebook.presto.hive.HiveUtil.columnExtraInfo;
import static com.facebook.presto.hive.HiveWriteUtils.createDirectory;
import static com.facebook.presto.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.difference;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestHiveClient
{
    protected static final String TEMPORARY_TABLE_PREFIX = "tmp_presto_test_";

    protected static final String INVALID_DATABASE = "totally_invalid_database_name";
    protected static final String INVALID_TABLE = "totally_invalid_table_name";
    protected static final String INVALID_COLUMN = "totally_invalid_column_name";

    protected static final String TEST_SERVER_VERSION = "test_version";

    private static final Type ARRAY_TYPE = arrayType(createUnboundedVarcharType());
    private static final Type MAP_TYPE = mapType(createUnboundedVarcharType(), BIGINT);
    private static final Type ROW_TYPE = rowType(ImmutableList.of(
            new NamedTypeSignature(Optional.of(new RowFieldName("f_string", false)), createUnboundedVarcharType().getTypeSignature()),
            new NamedTypeSignature(Optional.of(new RowFieldName("f_bigint", false)), BIGINT.getTypeSignature()),
            new NamedTypeSignature(Optional.of(new RowFieldName("f_boolean", false)), BOOLEAN.getTypeSignature())));

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

    private static final String CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE = "2015-07-04";

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA_2ND =
            MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE, createUnboundedVarcharType())
                    .row(4L, "hello", (byte) 45, (short) 345, 234, 123L, 754.1985f, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1L, true), CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE)
                    .row(5L, null, null, null, null, null, null, null, null, null, null, null, CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE)
                    .row(6L, "bye", (byte) 46, (short) 346, 345, 456L, -754.2008f, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0L, false), CREATE_TABLE_PARTITIONED_DATA_2ND_PARTITION_VALUE)
                    .build();

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("tinyint_to_smallint", TINYINT))
            .add(new ColumnMetadata("tinyint_to_integer", TINYINT))
            .add(new ColumnMetadata("tinyint_to_bigint", TINYINT))
            .add(new ColumnMetadata("smallint_to_integer", SMALLINT))
            .add(new ColumnMetadata("smallint_to_bigint", SMALLINT))
            .add(new ColumnMetadata("integer_to_bigint", INTEGER))
            .add(new ColumnMetadata("integer_to_varchar", INTEGER))
            .add(new ColumnMetadata("varchar_to_integer", createUnboundedVarcharType()))
            .add(new ColumnMetadata("float_to_double", REAL))
            .add(new ColumnMetadata("varchar_to_drop_in_row", createUnboundedVarcharType()))
            .build();

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_TABLE_BEFORE = ImmutableList.<ColumnMetadata>builder()
            .addAll(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE)
            .add(new ColumnMetadata("struct_to_struct", toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE)))
            .add(new ColumnMetadata("list_to_list", arrayType(toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE))))
            .add(new ColumnMetadata("map_to_map", mapType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE.get(1).getType(), toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_BEFORE))))
            .add(new ColumnMetadata("ds", createUnboundedVarcharType()))
            .build();

    private static RowType toRowType(List<ColumnMetadata> columns)
    {
        return rowType(columns.stream()
                .map(col -> new NamedTypeSignature(Optional.of(new RowFieldName(format("f_%s", col.getName()), false)), col.getType().getTypeSignature()))
                .collect(toList()));
    }

    private static final MaterializedResult MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_BEFORE =
            MaterializedResult.resultBuilder(SESSION, TINYINT, TINYINT, TINYINT, SMALLINT, SMALLINT, INTEGER, INTEGER, createUnboundedVarcharType(), REAL, createUnboundedVarcharType())
                    .row((byte) -11, (byte) 12, (byte) -13, (short) 14, (short) 15, -16, 17, "2147483647", 18.0f, "2016-08-01")
                    .row((byte) 21, (byte) -22, (byte) 23, (short) -24, (short) 25, 26, -27, "asdf", -28.0f, "2016-08-02")
                    .row((byte) -31, (byte) -32, (byte) 33, (short) 34, (short) -35, 36, 37, "-923", 39.5f, "2016-08-03")
                    .row(null, (byte) 42, (byte) 43, (short) 44, (short) -45, 46, 47, "2147483648", 49.5f, "2016-08-03")
                    .build();

    private static final MaterializedResult MISMATCH_SCHEMA_TABLE_DATA_BEFORE =
            MaterializedResult.resultBuilder(SESSION, MISMATCH_SCHEMA_TABLE_BEFORE.stream().map(ColumnMetadata::getType).collect(toList()))
                    .rows(MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_BEFORE.getMaterializedRows()
                            .stream()
                            .map(materializedRow -> {
                                List<Object> result = materializedRow.getFields();
                                List<Object> rowResult = materializedRow.getFields();
                                result.add(rowResult);
                                result.add(Arrays.asList(rowResult, null, rowResult));
                                result.add(ImmutableMap.of(rowResult.get(1), rowResult));
                                result.add(rowResult.get(9));
                                return new MaterializedRow(materializedRow.getPrecision(), result);
                            }).collect(toList()))
                    .build();

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("tinyint_to_smallint", SMALLINT))
            .add(new ColumnMetadata("tinyint_to_integer", INTEGER))
            .add(new ColumnMetadata("tinyint_to_bigint", BIGINT))
            .add(new ColumnMetadata("smallint_to_integer", INTEGER))
            .add(new ColumnMetadata("smallint_to_bigint", BIGINT))
            .add(new ColumnMetadata("integer_to_bigint", BIGINT))
            .add(new ColumnMetadata("integer_to_varchar", createUnboundedVarcharType()))
            .add(new ColumnMetadata("varchar_to_integer", INTEGER))
            .add(new ColumnMetadata("float_to_double", DOUBLE))
            .add(new ColumnMetadata("varchar_to_drop_in_row", createUnboundedVarcharType()))
            .build();

    private static final Type MISMATCH_SCHEMA_ROW_TYPE_APPEND = toRowType(ImmutableList.<ColumnMetadata>builder()
            .addAll(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER)
            .add(new ColumnMetadata(format("%s_append", MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.get(0).getName()), MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.get(0).getType()))
            .build());
    private static final Type MISMATCH_SCHEMA_ROW_TYPE_DROP = toRowType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.subList(0, MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.size() - 1));

    private static final List<ColumnMetadata> MISMATCH_SCHEMA_TABLE_AFTER = ImmutableList.<ColumnMetadata>builder()
            .addAll(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER)
            .add(new ColumnMetadata("struct_to_struct", MISMATCH_SCHEMA_ROW_TYPE_APPEND))
            .add(new ColumnMetadata("list_to_list", arrayType(MISMATCH_SCHEMA_ROW_TYPE_APPEND)))
            .add(new ColumnMetadata("map_to_map", mapType(MISMATCH_SCHEMA_PRIMITIVE_COLUMN_AFTER.get(1).getType(), MISMATCH_SCHEMA_ROW_TYPE_DROP)))
            .add(new ColumnMetadata("ds", createUnboundedVarcharType()))
            .build();

    private static final MaterializedResult MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_AFTER =
            MaterializedResult.resultBuilder(SESSION, SMALLINT, INTEGER, BIGINT, INTEGER, BIGINT, BIGINT, createUnboundedVarcharType(), INTEGER, DOUBLE, createUnboundedVarcharType())
                    .row((short) -11, 12, -13L, 14, 15L, -16L, "17", 2147483647, 18.0, "2016-08-01")
                    .row((short) 21, -22, 23L, -24, 25L, 26L, "-27", null, -28.0, "2016-08-02")
                    .row((short) -31, -32, 33L, 34, -35L, 36L, "37", -923, 39.5, "2016-08-03")
                    .row(null, 42, 43L, 44, -45L, 46L, "47", null, 49.5, "2016-08-03")
                    .build();

    private static final MaterializedResult MISMATCH_SCHEMA_TABLE_DATA_AFTER =
            MaterializedResult.resultBuilder(SESSION, MISMATCH_SCHEMA_TABLE_AFTER.stream().map(ColumnMetadata::getType).collect(toList()))
                    .rows(MISMATCH_SCHEMA_PRIMITIVE_FIELDS_DATA_AFTER.getMaterializedRows()
                            .stream()
                            .map(materializedRow -> {
                                List<Object> result = materializedRow.getFields();
                                List<Object> appendFieldRowResult = materializedRow.getFields();
                                appendFieldRowResult.add(null);
                                List<Object> dropFieldRowResult = materializedRow.getFields().subList(0, materializedRow.getFields().size() - 1);
                                result.add(appendFieldRowResult);
                                result.add(Arrays.asList(appendFieldRowResult, null, appendFieldRowResult));
                                result.add(ImmutableMap.of(result.get(1), dropFieldRowResult));
                                result.add(result.get(9));
                                return new MaterializedRow(materializedRow.getPrecision(), result);
                            }).collect(toList()))
                    .build();

    protected Set<HiveStorageFormat> createTableFormats = difference(ImmutableSet.copyOf(HiveStorageFormat.values()), ImmutableSet.of(AVRO));

    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());

    private static final List<ColumnMetadata> STATISTICS_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("t_boolean", BOOLEAN))
            .add(new ColumnMetadata("t_bigint", BIGINT))
            .add(new ColumnMetadata("t_integer", INTEGER))
            .add(new ColumnMetadata("t_smallint", SMALLINT))
            .add(new ColumnMetadata("t_tinyint", TINYINT))
            .add(new ColumnMetadata("t_double", DOUBLE))
            .add(new ColumnMetadata("t_float", REAL))
            .add(new ColumnMetadata("t_string", createUnboundedVarcharType()))
            .add(new ColumnMetadata("t_varchar", createVarcharType(100)))
            .add(new ColumnMetadata("t_char", createCharType(5)))
            .add(new ColumnMetadata("t_varbinary", VARBINARY))
            .add(new ColumnMetadata("t_date", DATE))
            .add(new ColumnMetadata("t_timestamp", TIMESTAMP))
            .add(new ColumnMetadata("t_short_decimal", createDecimalType(5, 2)))
            .add(new ColumnMetadata("t_long_decimal", createDecimalType(20, 4)))
            .build();

    private static final List<ColumnMetadata> STATISTICS_PARTITIONED_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .addAll(STATISTICS_TABLE_COLUMNS)
            .add(new ColumnMetadata("ds", VARCHAR))
            .build();

    protected static final PartitionStatistics EMPTY_TABLE_STATISTICS = new PartitionStatistics(createZeroStatistics(), ImmutableMap.of());
    protected static final PartitionStatistics BASIC_STATISTICS_1 = new PartitionStatistics(new HiveBasicStatistics(0, 2, 3, 0), ImmutableMap.of());
    protected static final PartitionStatistics BASIC_STATISTICS_2 = new PartitionStatistics(new HiveBasicStatistics(0, 3, 2, 0), ImmutableMap.of());

    private static final PartitionStatistics STATISTICS_1 =
            new PartitionStatistics(
                    BASIC_STATISTICS_1.getBasicStatistics(),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_boolean", createBooleanColumnStatistics(OptionalLong.of(5), OptionalLong.of(6), OptionalLong.of(3)))
                            .put("t_bigint", createIntegerColumnStatistics(OptionalLong.of(1234L), OptionalLong.of(5678L), OptionalLong.of(2), OptionalLong.of(5)))
                            .put("t_integer", createIntegerColumnStatistics(OptionalLong.of(123L), OptionalLong.of(567L), OptionalLong.of(3), OptionalLong.of(4)))
                            .put("t_smallint", createIntegerColumnStatistics(OptionalLong.of(12L), OptionalLong.of(56L), OptionalLong.of(2), OptionalLong.of(6)))
                            .put("t_tinyint", createIntegerColumnStatistics(OptionalLong.of(1L), OptionalLong.of(2L), OptionalLong.of(1), OptionalLong.of(3)))
                            .put("t_double", createDoubleColumnStatistics(OptionalDouble.of(1234.25), OptionalDouble.of(5678.58), OptionalLong.of(7), OptionalLong.of(8)))
                            .put("t_float", createDoubleColumnStatistics(OptionalDouble.of(123.25), OptionalDouble.of(567.58), OptionalLong.of(9), OptionalLong.of(10)))
                            .put("t_string", createStringColumnStatistics(OptionalLong.of(10), OptionalDouble.of(5.0), OptionalLong.of(3), OptionalLong.of(7)))
                            .put("t_varchar", createStringColumnStatistics(OptionalLong.of(100), OptionalDouble.of(23.3), OptionalLong.of(5), OptionalLong.of(3)))
                            .put("t_char", createStringColumnStatistics(OptionalLong.of(5), OptionalDouble.of(5.0), OptionalLong.of(1), OptionalLong.of(4)))
                            .put("t_varbinary", createBinaryColumnStatistics(OptionalLong.of(4), OptionalDouble.of(3.0), OptionalLong.of(1)))
                            .put("t_date", createDateColumnStatistics(Optional.of(java.time.LocalDate.ofEpochDay(1)), Optional.of(java.time.LocalDate.ofEpochDay(2)), OptionalLong.of(7), OptionalLong.of(6)))
                            .put("t_timestamp", createIntegerColumnStatistics(OptionalLong.of(1234567L), OptionalLong.of(71234567L), OptionalLong.of(7), OptionalLong.of(5)))
                            .put("t_short_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal(10)), Optional.of(new BigDecimal(12)), OptionalLong.of(3), OptionalLong.of(5)))
                            .put("t_long_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal("12345678901234567.123")), Optional.of(new BigDecimal("812345678901234567.123")), OptionalLong.of(2), OptionalLong.of(1)))
                            .build());

    private static final PartitionStatistics STATISTICS_1_1 =
            new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(2), OptionalLong.empty(), OptionalLong.of(0)),
                    STATISTICS_1.getColumnStatistics().entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().hashCode() % 2 == 0)
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

    private static final PartitionStatistics STATISTICS_1_2 =
            new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.empty(), OptionalLong.of(3), OptionalLong.of(0)),
                    STATISTICS_1.getColumnStatistics().entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().hashCode() % 2 == 1)
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

    private static final PartitionStatistics STATISTICS_2 =
            new PartitionStatistics(
                    BASIC_STATISTICS_2.getBasicStatistics(),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_boolean", createBooleanColumnStatistics(OptionalLong.of(4), OptionalLong.of(3), OptionalLong.of(2)))
                            .put("t_bigint", createIntegerColumnStatistics(OptionalLong.of(2345L), OptionalLong.of(6789L), OptionalLong.of(4), OptionalLong.of(7)))
                            .put("t_integer", createIntegerColumnStatistics(OptionalLong.of(234L), OptionalLong.of(678L), OptionalLong.of(5), OptionalLong.of(6)))
                            .put("t_smallint", createIntegerColumnStatistics(OptionalLong.of(23L), OptionalLong.of(65L), OptionalLong.of(7), OptionalLong.of(5)))
                            .put("t_tinyint", createIntegerColumnStatistics(OptionalLong.of(12), OptionalLong.of(3L), OptionalLong.of(2), OptionalLong.of(3)))
                            .put("t_double", createDoubleColumnStatistics(OptionalDouble.of(2345.25), OptionalDouble.of(6785.58), OptionalLong.of(6), OptionalLong.of(3)))
                            .put("t_float", createDoubleColumnStatistics(OptionalDouble.of(235.25), OptionalDouble.of(676.58), OptionalLong.of(7), OptionalLong.of(11)))
                            .put("t_string", createStringColumnStatistics(OptionalLong.of(11), OptionalDouble.of(6.0), OptionalLong.of(2), OptionalLong.of(6)))
                            .put("t_varchar", createStringColumnStatistics(OptionalLong.of(99), OptionalDouble.of(22.3), OptionalLong.of(7), OptionalLong.of(1)))
                            .put("t_char", createStringColumnStatistics(OptionalLong.of(6), OptionalDouble.of(6.0), OptionalLong.of(0), OptionalLong.of(3)))
                            .put("t_varbinary", createBinaryColumnStatistics(OptionalLong.of(2), OptionalDouble.of(1.0), OptionalLong.of(2)))
                            .put("t_date", createDateColumnStatistics(Optional.of(java.time.LocalDate.ofEpochDay(2)), Optional.of(java.time.LocalDate.ofEpochDay(3)), OptionalLong.of(8), OptionalLong.of(7)))
                            .put("t_timestamp", createIntegerColumnStatistics(OptionalLong.of(2345671L), OptionalLong.of(12345677L), OptionalLong.of(9), OptionalLong.of(1)))
                            .put("t_short_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal(11)), Optional.of(new BigDecimal(14)), OptionalLong.of(5), OptionalLong.of(7)))
                            .put("t_long_decimal", createDecimalColumnStatistics(Optional.of(new BigDecimal("71234567890123456.123")), Optional.of(new BigDecimal("781234567890123456.123")), OptionalLong.of(2), OptionalLong.of(1)))
                            .build());

    private static final PartitionStatistics STATISTICS_EMPTY_OPTIONAL_FIELDS =
            new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(0)),
                    ImmutableMap.<String, HiveColumnStatistics>builder()
                            .put("t_boolean", createBooleanColumnStatistics(OptionalLong.of(4), OptionalLong.of(3), OptionalLong.of(2)))
                            .put("t_bigint", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(4), OptionalLong.of(7)))
                            .put("t_integer", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(5), OptionalLong.of(6)))
                            .put("t_smallint", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(7), OptionalLong.of(5)))
                            .put("t_tinyint", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(2), OptionalLong.of(3)))
                            .put("t_double", createDoubleColumnStatistics(OptionalDouble.empty(), OptionalDouble.empty(), OptionalLong.of(6), OptionalLong.of(3)))
                            .put("t_float", createDoubleColumnStatistics(OptionalDouble.empty(), OptionalDouble.empty(), OptionalLong.of(7), OptionalLong.of(11)))
                            .put("t_string", createStringColumnStatistics(OptionalLong.of(0), OptionalDouble.of(0), OptionalLong.of(2), OptionalLong.of(6)))
                            .put("t_varchar", createStringColumnStatistics(OptionalLong.of(0), OptionalDouble.of(0), OptionalLong.of(7), OptionalLong.of(1)))
                            .put("t_char", createStringColumnStatistics(OptionalLong.of(0), OptionalDouble.of(0), OptionalLong.of(0), OptionalLong.of(3)))
                            .put("t_varbinary", createBinaryColumnStatistics(OptionalLong.of(0), OptionalDouble.of(0), OptionalLong.of(2)))
                            // https://issues.apache.org/jira/browse/HIVE-20098
                            // .put("t_date", createDateColumnStatistics(Optional.empty(), Optional.empty(), OptionalLong.of(8), OptionalLong.of(7)))
                            .put("t_timestamp", createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(9), OptionalLong.of(1)))
                            .put("t_short_decimal", createDecimalColumnStatistics(Optional.empty(), Optional.empty(), OptionalLong.of(5), OptionalLong.of(7)))
                            .put("t_long_decimal", createDecimalColumnStatistics(Optional.empty(), Optional.empty(), OptionalLong.of(2), OptionalLong.of(1)))
                            .build());

    protected String clientId;
    protected String database;
    protected SchemaTableName tablePartitionFormat;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName tableOffline;
    protected SchemaTableName tableOfflinePartition;
    protected SchemaTableName tableNotReadable;
    protected SchemaTableName view;
    protected SchemaTableName invalidTable;
    protected SchemaTableName tableBucketedStringInt;
    protected SchemaTableName tableBucketedBigintBoolean;
    protected SchemaTableName tableBucketedDoubleFloat;
    protected SchemaTableName tablePartitionSchemaChange;
    protected SchemaTableName tablePartitionSchemaChangeNonCanonical;
    protected SchemaTableName tableBucketEvolution;

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
    {
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
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
        tableNotReadable = new SchemaTableName(database, "presto_test_not_readable");
        view = new SchemaTableName(database, "presto_test_view");
        invalidTable = new SchemaTableName(database, INVALID_TABLE);
        tableBucketedStringInt = new SchemaTableName(database, "presto_test_bucketed_by_string_int");
        tableBucketedBigintBoolean = new SchemaTableName(database, "presto_test_bucketed_by_bigint_boolean");
        tableBucketedDoubleFloat = new SchemaTableName(database, "presto_test_bucketed_by_double_float");
        tablePartitionSchemaChange = new SchemaTableName(database, "presto_test_partition_schema_change");
        tablePartitionSchemaChangeNonCanonical = new SchemaTableName(database, "presto_test_partition_schema_change_non_canonical");
        tableBucketEvolution = new SchemaTableName(database, "presto_test_bucket_evolution");

        invalidClientId = "hive";
        invalidTableHandle = new HiveTableHandle(database, INVALID_TABLE);
        invalidTableLayoutHandle = new HiveTableLayoutHandle(
                invalidTable,
                ImmutableList.of(),
                ImmutableList.of(new HivePartition(invalidTable, "unknown", ImmutableMap.of())),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty());

        dsColumn = new HiveColumnHandle("ds", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, PARTITION_KEY, Optional.empty());
        fileFormatColumn = new HiveColumnHandle("file_format", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, PARTITION_KEY, Optional.empty());
        dummyColumn = new HiveColumnHandle("dummy", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), -1, PARTITION_KEY, Optional.empty());
        intColumn = new HiveColumnHandle("t_int", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), -1, PARTITION_KEY, Optional.empty());
        invalidColumnHandle = new HiveColumnHandle(INVALID_COLUMN, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 0, REGULAR, Optional.empty());

        List<ColumnHandle> partitionColumns = ImmutableList.of(dsColumn, fileFormatColumn, dummyColumn);
        List<HivePartition> partitions = ImmutableList.<HivePartition>builder()
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=textfile/dummy=1",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("textfile")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 1L))
                                .build()))
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=sequencefile/dummy=2",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("sequencefile")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 2L))
                                .build()))
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=rctext/dummy=3",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("rctext")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 3L))
                                .build()))
                .add(new HivePartition(tablePartitionFormat,
                        "ds=2012-12-29/file_format=rcbinary/dummy=4",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("rcbinary")))
                                .put(dummyColumn, NullableValue.of(INTEGER, 4L))
                                .build()))
                .build();
        partitionCount = partitions.size();
        tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumn, NullableValue.of(createUnboundedVarcharType(), utf8Slice("2012-12-29"))));
        tableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(tablePartitionFormat, partitionColumns, partitions, tupleDomain, tupleDomain, Optional.empty(), Optional.empty()),
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
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(INTEGER, 4L)), false)))))),
                ImmutableList.of());
        List<HivePartition> unpartitionedPartitions = ImmutableList.of(new HivePartition(tableUnpartitioned));
        unpartitionedTableLayout = new ConnectorTableLayout(new HiveTableLayoutHandle(tableUnpartitioned, ImmutableList.of(), unpartitionedPartitions, TupleDomain.all(), TupleDomain.all(), Optional.empty(), Optional.empty()));
        timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneId));
    }

    protected final void setup(String host, int port, String databaseName, String timeZone)
    {
        HiveClientConfig hiveClientConfig = getHiveClientConfig();
        hiveClientConfig.setTimeZone(timeZone);
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        ExtendedHiveMetastore metastore = new CachingHiveMetastore(
                new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster)),
                executor,
                Duration.valueOf("1m"),
                Duration.valueOf("15s"),
                10000);

        setup(databaseName, hiveClientConfig, metastore);
    }

    protected final void setup(String databaseName, HiveClientConfig hiveClientConfig, ExtendedHiveMetastore hiveMetastore)
    {
        HiveConnectorId connectorId = new HiveConnectorId("hive-test");

        setupHive(connectorId.toString(), databaseName, hiveClientConfig.getTimeZone());

        metastoreClient = hiveMetastore;
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(hiveClientConfig));
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig, new NoHdfsAuthentication());
        locationService = new HiveLocationService(hdfsEnvironment);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        metadataFactory = new HiveMetadataFactory(
                metastoreClient,
                hdfsEnvironment,
                new HivePartitionManager(TYPE_MANAGER, hiveClientConfig),
                timeZone,
                10,
                true,
                false,
                false,
                true,
                1000,
                getHiveClientConfig().getMaxPartitionsPerScan(),
                TYPE_MANAGER,
                locationService,
                new TableParameterCodec(),
                partitionUpdateCodec,
                newFixedThreadPool(2),
                new HiveTypeTranslator(),
                TEST_SERVER_VERSION);
        transactionManager = new HiveTransactionManager();
        splitManager = new HiveSplitManager(
                transactionHandle -> ((HiveMetadata) transactionManager.get(transactionHandle)).getMetastore(),
                new NamenodeStats(),
                hdfsEnvironment,
                new HadoopDirectoryLister(),
                directExecutor(),
                new HiveCoercionPolicy(TYPE_MANAGER),
                new CounterStat(),
                100,
                hiveClientConfig.getMaxOutstandingSplitsSize(),
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxInitialSplits(),
                hiveClientConfig.getSplitLoaderConcurrency(),
                false);
        pageSinkProvider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(hiveClientConfig),
                hdfsEnvironment,
                PAGE_SORTER,
                metastoreClient,
                new GroupByHashPageIndexerFactory(JOIN_COMPILER),
                TYPE_MANAGER,
                getHiveClientConfig(),
                locationService,
                partitionUpdateCodec,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                new HiveSessionProperties(hiveClientConfig, new OrcFileWriterConfig()),
                new HiveWriterStats());
        pageSourceProvider = new HivePageSourceProvider(hiveClientConfig, hdfsEnvironment, getDefaultHiveRecordCursorProvider(hiveClientConfig), getDefaultHiveDataStreamFactories(hiveClientConfig), TYPE_MANAGER);
    }

    /**
     * Allow subclass to change default configuration.
     */
    protected HiveClientConfig getHiveClientConfig()
    {
        return new HiveClientConfig();
    }

    protected ConnectorSession newSession()
    {
        return new TestingConnectorSession(new HiveSessionProperties(getHiveClientConfig(), new OrcFileWriterConfig()).getSessionProperties());
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
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<String> databases = metadata.listSchemaNames(newSession());
            assertTrue(databases.contains(database));
        }
    }

    @Test
    public void testGetTableNames()
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
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, Constraint.alwaysTrue(), Optional.empty());
            assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
        }
    }

    @Test
    public void testGetPartitionsWithBindings()
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
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.getTableLayouts(newSession(), invalidTableHandle, Constraint.alwaysTrue(), Optional.empty());
        }
    }

    @Test
    public void testGetPartitionNames()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, Constraint.alwaysTrue(), Optional.empty());
            assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
        }
    }

    @Test
    public void testMismatchSchemaTable()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            // TODO: fix coercion for JSON
            if (storageFormat == JSON) {
                continue;
            }
            SchemaTableName temporaryMismatchSchemaTable = temporaryTable("mismatch_schema");
            try {
                doTestMismatchSchemaTable(
                        temporaryMismatchSchemaTable,
                        storageFormat,
                        MISMATCH_SCHEMA_TABLE_BEFORE,
                        MISMATCH_SCHEMA_TABLE_DATA_BEFORE,
                        MISMATCH_SCHEMA_TABLE_AFTER,
                        MISMATCH_SCHEMA_TABLE_DATA_AFTER);
            }
            finally {
                dropTable(temporaryMismatchSchemaTable);
            }
        }
    }

    protected void doTestMismatchSchemaTable(
            SchemaTableName schemaTableName,
            HiveStorageFormat storageFormat,
            List<ColumnMetadata> tableBefore,
            MaterializedResult dataBefore,
            List<ColumnMetadata> tableAfter,
            MaterializedResult dataAfter)
            throws Exception
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        doCreateEmptyTable(schemaTableName, storageFormat, tableBefore);

        // insert the data
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);

            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(dataBefore.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            metadata.finishInsert(session, insertTableHandle, fragments);

            transaction.commit();
        }

        // load the table and verify the data
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);

            List<ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle).values().stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toList());

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), dataBefore.getMaterializedRows());
            transaction.commit();
        }

        // alter the table schema
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            PrincipalPrivileges principalPrivileges = testingPrincipalPrivilege(session.getUser());
            Table oldTable = transaction.getMetastore(schemaName).getTable(schemaName, tableName).get();
            HiveTypeTranslator hiveTypeTranslator = new HiveTypeTranslator();
            List<Column> dataColumns = tableAfter.stream()
                    .filter(columnMetadata -> !columnMetadata.getName().equals("ds"))
                    .map(columnMetadata -> new Column(columnMetadata.getName(), toHiveType(hiveTypeTranslator, columnMetadata.getType()), Optional.empty()))
                    .collect(toList());
            Table.Builder newTable = Table.builder(oldTable)
                    .setDataColumns(dataColumns);

            transaction.getMetastore(schemaName).replaceView(schemaName, tableName, newTable.build(), principalPrivileges);

            transaction.commit();
        }

        // load the altered table and verify the data
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);
            List<ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle).values().stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toList());

            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), dataAfter.getMaterializedRows());

            transaction.commit();
        }

        // insertions to the partitions with type mismatches should fail
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, schemaTableName);

            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(dataAfter.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());

            metadata.finishInsert(session, insertTableHandle, fragments);

            transaction.commit();

            fail("expected exception");
        }
        catch (PrestoException e) {
            // expected
            assertEquals(e.getErrorCode(), HIVE_PARTITION_SCHEMA_MISMATCH.toErrorCode());
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
        assertExpectedPartitions(actual.getPartitions().get(), expected.getPartitions().get());
    }

    protected void assertExpectedPartitions(List<HivePartition> actualPartitions, Iterable<HivePartition> expectedPartitions)
    {
        Map<String, ?> actualById = uniqueIndex(actualPartitions, HivePartition::getPartitionId);
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
        }
    }

    @Test
    public void testGetPartitionNamesUnpartitioned()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, Constraint.alwaysTrue(), Optional.empty());
            assertEquals(getAllPartitions(getOnlyElement(tableLayoutResults).getTableLayout().getHandle()).size(), 1);
            assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), unpartitionedTableLayout);
        }
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            metadata.getTableLayouts(newSession(), invalidTableHandle, Constraint.alwaysTrue(), Optional.empty());
        }
    }

    @SuppressWarnings({"ValueOfIncrementOrDecrementUsed", "UnusedAssignment"})
    @Test
    public void testGetTableSchemaPartitionFormat()
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
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(newSession(), tableOffline.toSchemaTablePrefix());
            assertEquals(columns.size(), 1);
            Map<String, ColumnMetadata> map = uniqueIndex(getOnlyElement(columns.values()), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
        }
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
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
    public void testGetTableSchemaNotReadablePartition()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableNotReadable);
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

            assertPrimitiveField(map, "t_string", createUnboundedVarcharType(), false);
        }
    }

    @Test
    public void testGetTableSchemaException()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertNull(metadata.getTableHandle(newSession(), invalidTable));
        }
    }

    @Test
    public void testGetTableStatsBucketedStringInt()
    {
        assertTableStatsComputed(
                tableBucketedStringInt,
                ImmutableSet.of(
                        "t_bigint",
                        "t_boolean",
                        "t_double",
                        "t_float",
                        "t_int",
                        "t_smallint",
                        "t_string",
                        "t_tinyint",
                        "ds"));
    }

    @Test
    public void testGetTableStatsUnpartitioned()
    {
        assertTableStatsComputed(
                tableUnpartitioned,
                ImmutableSet.of("t_string", "t_tinyint"));
    }

    private void assertTableStatsComputed(
            SchemaTableName tableName,
            Set<String> expectedColumnStatsColumns)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, Constraint.alwaysTrue());

            assertFalse(tableStatistics.getRowCount().isValueUnknown(), "row count is unknown");

            Map<String, ColumnStatistics> columnsStatistics = tableStatistics
                    .getColumnStatistics()
                    .entrySet()
                    .stream()
                    .collect(
                            toImmutableMap(
                                    entry -> ((HiveColumnHandle) entry.getKey()).getName(),
                                    Map.Entry::getValue));

            assertEquals(columnsStatistics.keySet(), expectedColumnStatsColumns, "columns with statistics");

            columnsStatistics.forEach((columnName, columnStatistics) -> {
                assertFalse(
                        columnStatistics.getNullsFraction().isValueUnknown(),
                        "unknown nulls fraction for " + columnName);

                RangeColumnStatistics rangeColumnStatistics = columnStatistics.getOnlyRangeColumnStatistics();
                assertFalse(
                        rangeColumnStatistics.getDistinctValuesCount().isValueUnknown(),
                        "unknown range distinct values count for " + columnName);
                assertFalse(
                        rangeColumnStatistics.getFraction().isValueUnknown(),
                        "unknown range non-null fraction for " + columnName);
            });
        }
    }

    @Test
    public void testGetPartitionSplitsBatch()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, Constraint.alwaysTrue(), Optional.empty());
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle(), UNGROUPED_SCHEDULING);

            assertEquals(getSplitCount(splitSource), partitionCount);
        }
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, Constraint.alwaysTrue(), Optional.empty());
            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle(), UNGROUPED_SCHEDULING);

            assertEquals(getSplitCount(splitSource), 1);
        }
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
    {
        try (Transaction transaction = newTransaction()) {
            splitManager.getSplits(transaction.getTransactionHandle(), newSession(), invalidTableLayoutHandle, UNGROUPED_SCHEDULING);
        }
    }

    @Test
    public void testGetPartitionTableOffline()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            try {
                getTableHandle(metadata, tableOffline);
                fail("expected TableOfflineException");
            }
            catch (TableOfflineException e) {
                assertEquals(e.getTableName(), tableOffline);
            }
        }
    }

    @Test
    public void testGetPartitionSplitsTableOfflinePartition()
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
                getSplitCount(splitManager.getSplits(transaction.getTransactionHandle(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle(), UNGROUPED_SCHEDULING));
                fail("Expected PartitionOfflineException");
            }
            catch (PartitionOfflineException e) {
                assertEquals(e.getTableName(), tableOfflinePartition);
                assertEquals(e.getPartition(), "ds=2012-12-30");
            }
        }
    }

    @Test
    public void testGetPartitionSplitsTableNotReadablePartition()
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableNotReadable);
            assertNotNull(tableHandle);

            ColumnHandle dsColumn = metadata.getColumnHandles(session, tableHandle).get("ds");
            assertNotNull(dsColumn);

            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, Constraint.alwaysTrue(), Optional.empty());
            try {
                getSplitCount(splitManager.getSplits(transaction.getTransactionHandle(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle(), UNGROUPED_SCHEDULING));
                fail("Expected HiveNotReadableException");
            }
            catch (HiveNotReadableException e) {
                assertThat(e).hasMessageMatching("Table '.*\\.presto_test_not_readable' is not readable: reason for not readable");
                assertEquals(e.getTableName(), tableNotReadable);
                assertEquals(e.getPartition(), Optional.empty());
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

    @Test
    public void testBucketedTableEvolution()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryBucketEvolutionTable = temporaryTable("bucket_evolution");
            try {
                doTestBucketedTableEvolution(storageFormat, temporaryBucketEvolutionTable);
            }
            finally {
                dropTable(temporaryBucketEvolutionTable);
            }
        }
    }

    private void doTestBucketedTableEvolution(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        int rowCount = 100;

        //
        // Produce a table with 8 buckets.
        // The table has 3 partitions of 3 different bucket count (4, 8, 16).
        createEmptyTable(
                tableName,
                storageFormat,
                ImmutableList.of(
                        new Column("id", HIVE_LONG, Optional.empty()),
                        new Column("name", HIVE_STRING, Optional.empty())),
                ImmutableList.of(new Column("pk", HIVE_STRING, Optional.empty())),
                Optional.of(new HiveBucketProperty(ImmutableList.of("id"), 4, ImmutableList.of())));
        // write a 4-bucket partition
        MaterializedResult.Builder bucket4Builder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> bucket4Builder.row((long) i, String.valueOf(i), "four"));
        insertData(tableName, bucket4Builder.build());
        // write a 16-bucket partition
        alterBucketProperty(tableName, Optional.of(new HiveBucketProperty(ImmutableList.of("id"), 16, ImmutableList.of())));
        MaterializedResult.Builder bucket16Builder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> bucket16Builder.row((long) i, String.valueOf(i), "sixteen"));
        insertData(tableName, bucket16Builder.build());
        // write an 8-bucket partition
        alterBucketProperty(tableName, Optional.of(new HiveBucketProperty(ImmutableList.of("id"), 8, ImmutableList.of())));
        MaterializedResult.Builder bucket8Builder = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, VARCHAR);
        IntStream.range(0, rowCount).forEach(i -> bucket8Builder.row((long) i, String.valueOf(i), "eight"));
        insertData(tableName, bucket8Builder.build());

        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // read entire table
            List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>builder()
                    .addAll(metadata.getColumnHandles(session, tableHandle).values())
                    .build();
            MaterializedResult result = readTable(
                    transaction,
                    tableHandle,
                    columnHandles,
                    session,
                    TupleDomain.all(),
                    OptionalInt.empty(),
                    Optional.empty());
            assertBucketTableEvolutionResult(result, columnHandles, ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7), rowCount);

            // read single bucket (table/logical bucket)
            result = readTable(
                    transaction,
                    tableHandle,
                    columnHandles,
                    session,
                    TupleDomain.fromFixedValues(ImmutableMap.of(bucketColumnHandle(), NullableValue.of(INTEGER, 6L))),
                    OptionalInt.empty(),
                    Optional.empty());
            assertBucketTableEvolutionResult(result, columnHandles, ImmutableSet.of(6), rowCount);

            // read single bucket, without selecting the bucketing column (i.e. id column)
            columnHandles = ImmutableList.<ColumnHandle>builder()
                    .addAll(metadata.getColumnHandles(session, tableHandle).values().stream()
                            .filter(columnHandle -> !"id".equals(((HiveColumnHandle) columnHandle).getName()))
                            .collect(toImmutableList()))
                    .build();
            result = readTable(
                    transaction,
                    tableHandle,
                    columnHandles,
                    session,
                    TupleDomain.fromFixedValues(ImmutableMap.of(bucketColumnHandle(), NullableValue.of(INTEGER, 6L))),
                    OptionalInt.empty(),
                    Optional.empty());
            assertBucketTableEvolutionResult(result, columnHandles, ImmutableSet.of(6), rowCount);
        }
    }

    private static void assertBucketTableEvolutionResult(MaterializedResult result, List<ColumnHandle> columnHandles, Set<Integer> bucketIds, int rowCount)
    {
        // Assert that only elements in the specified bucket shows up, and each element shows up 3 times.
        int bucketCount = 8;
        Set<Long> expectedIds = LongStream.range(0, rowCount)
                .filter(x -> bucketIds.contains(toIntExact(x % bucketCount)))
                .boxed()
                .collect(toImmutableSet());

        // assert that content from all three buckets are the same
        Map<String, Integer> columnIndex = indexColumns(columnHandles);
        OptionalInt idColumnIndex = columnIndex.containsKey("id") ? OptionalInt.of(columnIndex.get("id")) : OptionalInt.empty();
        int nameColumnIndex = columnIndex.get("name");
        int bucketColumnIndex = columnIndex.get(BUCKET_COLUMN_NAME);
        Map<Long, Integer> idCount = new HashMap<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            String name = (String) row.getField(nameColumnIndex);
            int bucket = (int) row.getField(bucketColumnIndex);
            idCount.compute(Long.parseLong(name), (key, oldValue) -> oldValue == null ? 1 : oldValue + 1);
            assertEquals(bucket, Integer.parseInt(name) % bucketCount);
            if (idColumnIndex.isPresent()) {
                long id = (long) row.getField(idColumnIndex.getAsInt());
                assertEquals(Integer.parseInt(name), id);
            }
        }
        assertEquals(
                (int) idCount.values().stream()
                        .distinct()
                        .collect(onlyElement()),
                3);
        assertEquals(idCount.keySet(), expectedIds);
    }

    private void assertTableIsBucketed(ConnectorTableHandle tableHandle)
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

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*The column 't_data' in table '.*\\.presto_test_partition_schema_change' is declared as type 'double', but partition 'ds=2012-12-29' declared column 't_data' as type 'string'.")
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

            ConnectorSplitSource splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle, UNGROUPED_SCHEDULING);
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
    public void testTypesRcBinary()
            throws Exception
    {
        assertGetRecords("presto_test_types_rcbinary", RCBINARY);
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
    public void testEmptyTextFile()
            throws Exception
    {
        assertEmptyFile(TEXTFILE);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Error opening Hive split .*SequenceFile.*EOFException")
    public void testEmptySequenceFile()
            throws Exception
    {
        assertEmptyFile(SEQUENCEFILE);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "RCFile is empty: .*")
    public void testEmptyRcTextFile()
            throws Exception
    {
        assertEmptyFile(RCTEXT);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "RCFile is empty: .*")
    public void testEmptyRcBinaryFile()
            throws Exception
    {
        assertEmptyFile(RCBINARY);
    }

    @Test
    public void testEmptyOrcFile()
            throws Exception
    {
        assertEmptyFile(ORC);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "ORC file is empty: .*")
    public void testEmptyDwrfFile()
            throws Exception
    {
        assertEmptyFile(DWRF);
    }

    private void assertEmptyFile(HiveStorageFormat format)
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("empty_file");
        try {
            List<Column> columns = ImmutableList.of(new Column("test", HIVE_STRING, Optional.empty()));
            createEmptyTable(tableName, format, columns, ImmutableList.of());

            try (Transaction transaction = newTransaction()) {
                ConnectorSession session = newSession();
                ConnectorMetadata metadata = transaction.getMetadata();

                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

                Table table = transaction.getMetastore(tableName.getSchemaName())
                        .getTable(tableName.getSchemaName(), tableName.getTableName())
                        .orElseThrow(AssertionError::new);

                // verify directory is empty
                HdfsContext context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
                Path location = new Path(table.getStorage().getLocation());
                assertTrue(listDirectory(context, location).isEmpty());

                // read table with empty directory
                readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.of(0), Optional.of(ORC));

                // create empty file
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, location);
                assertTrue(fileSystem.createNewFile(new Path(location, "empty-file")));
                assertEquals(listDirectory(context, location), ImmutableList.of("empty-file"));

                // read table with empty file
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.of(1), Optional.empty());
                assertEquals(result.getRowCount(), 0);
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testHiveViewsAreNotSupported()
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
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            assertEquals(metadata.listTableColumns(newSession(), new SchemaTablePrefix(view.getSchemaName(), view.getTableName())), ImmutableMap.of());
        }
    }

    @Test
    public void testRenameTable()
    {
        SchemaTableName temporaryRenameTableOld = temporaryTable("rename_old");
        SchemaTableName temporaryRenameTableNew = temporaryTable("rename_new");
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
            SchemaTableName temporaryCreateTable = temporaryTable("create");
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
        SchemaTableName temporaryCreateRollbackTable = temporaryTable("create_rollback");
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
                HdfsContext context = new HdfsContext(session, temporaryCreateRollbackTable.getSchemaName(), temporaryCreateRollbackTable.getTableName());
                assertFalse(listAllDataFiles(context, stagingPathRoot).isEmpty());

                // rollback the table
                transaction.rollback();
            }

            // verify all files have been deleted
            HdfsContext context = new HdfsContext(newSession(), temporaryCreateRollbackTable.getSchemaName(), temporaryCreateRollbackTable.getTableName());
            assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());

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
    public void testTableCreationIgnoreExisting()
    {
        List<Column> columns = ImmutableList.of(new Column("dummy", HiveType.valueOf("uniontype<smallint,tinyint>"), Optional.empty()));
        SchemaTableName schemaTableName = temporaryTable("create");
        ConnectorSession session = newSession();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        PrincipalPrivileges privileges = testingPrincipalPrivilege(session.getUser());
        Path targetPath;
        try {
            try (Transaction transaction = newTransaction()) {
                LocationService locationService = getLocationService(schemaName);
                LocationHandle locationHandle = locationService.forNewTable(transaction.getMetastore(schemaName), session, schemaName, tableName);
                targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath, "q1");
                transaction.getMetastore(schemaName)
                        .createTable(session, table, privileges, Optional.empty(), false, EMPTY_TABLE_STATISTICS);
                Optional<Table> tableHandle = transaction.getMetastore(schemaName).getTable(schemaName, tableName);
                assertTrue(tableHandle.isPresent());
                transaction.commit();
            }

            // try creating it again from another transaction with ignoreExisting=false
            try (Transaction transaction = newTransaction()) {
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath.suffix("_2"), "q2");
                transaction.getMetastore(schemaName)
                        .createTable(session, table, privileges, Optional.empty(), false, EMPTY_TABLE_STATISTICS);
                transaction.commit();
                fail("Expected exception");
            }
            catch (PrestoException e) {
                assertInstanceOf(e, TableAlreadyExistsException.class);
            }

            // try creating it again from another transaction with ignoreExisting=true
            try (Transaction transaction = newTransaction()) {
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath.suffix("_3"), "q3");
                transaction.getMetastore(schemaName)
                        .createTable(session, table, privileges, Optional.empty(), true, EMPTY_TABLE_STATISTICS);
                transaction.commit();
            }

            // at this point the table should exist, now try creating the table again with a different table definition
            columns = ImmutableList.of(new Column("new_column", HiveType.valueOf("string"), Optional.empty()));
            try (Transaction transaction = newTransaction()) {
                Table table = createSimpleTable(schemaTableName, columns, session, targetPath.suffix("_4"), "q4");
                transaction.getMetastore(schemaName)
                        .createTable(session, table, privileges, Optional.empty(), true, EMPTY_TABLE_STATISTICS);
                transaction.commit();
                fail("Expected exception");
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
                assertEquals(e.getMessage(), format("Table already exists with a different schema: '%s'", schemaTableName.getTableName()));
            }
        }
        finally {
            dropTable(schemaTableName);
        }
    }

    private static Table createSimpleTable(SchemaTableName schemaTableName, List<Column> columns, ConnectorSession session, Path targetPath, String queryId)
    {
        String tableOwner = session.getUser();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(tableOwner)
                .setTableType(TableType.MANAGED_TABLE.name())
                .setParameters(ImmutableMap.of(
                        PRESTO_VERSION_NAME, TEST_SERVER_VERSION,
                        PRESTO_QUERY_ID_NAME, queryId))
                .setDataColumns(columns)
                .withStorage(storage -> storage
                        .setLocation(targetPath.toString())
                        .setStorageFormat(fromHiveStorageFormat(ORC))
                        .setSerdeParameters(ImmutableMap.of()))
                .build();
    }

    @Test
    public void testBucketSortedTables()
            throws Exception
    {
        SchemaTableName table = temporaryTable("create_sorted");
        try {
            doTestBucketSortedTables(table);
        }
        finally {
            dropTable(table);
        }
    }

    private void doTestBucketSortedTables(SchemaTableName table)
            throws IOException
    {
        int bucketCount = 3;

        try (Transaction transaction = newTransaction()) {
            HiveClientConfig hiveConfig = getHiveClientConfig()
                    .setSortedWritingEnabled(true)
                    .setWriterSortBufferSize(new DataSize(1, MEGABYTE));
            ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(hiveConfig, new OrcFileWriterConfig()).getSessionProperties());
            ConnectorMetadata metadata = transaction.getMetadata();

            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                    table,
                    ImmutableList.<ColumnMetadata>builder()
                            .add(new ColumnMetadata("id", VARCHAR))
                            .add(new ColumnMetadata("value_asc", VARCHAR))
                            .add(new ColumnMetadata("value_desc", BIGINT))
                            .add(new ColumnMetadata("ds", VARCHAR))
                            .build(),
                    ImmutableMap.<String, Object>builder()
                            .put(STORAGE_FORMAT_PROPERTY, RCBINARY)
                            .put(PARTITIONED_BY_PROPERTY, ImmutableList.of("ds"))
                            .put(BUCKETED_BY_PROPERTY, ImmutableList.of("id"))
                            .put(BUCKET_COUNT_PROPERTY, bucketCount)
                            .put(SORTED_BY_PROPERTY, ImmutableList.builder()
                                    .add(new SortingColumn("value_asc", SortingColumn.Order.ASCENDING))
                                    .add(new SortingColumn("value_desc", SortingColumn.Order.DESCENDING))
                                    .build())
                            .build());

            ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());

            // write the data
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, outputHandle);
            List<Type> types = tableMetadata.getColumns().stream()
                    .map(ColumnMetadata::getType)
                    .collect(toList());
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < 200; i++) {
                MaterializedResult.Builder builder = MaterializedResult.resultBuilder(session, types);
                for (int j = 0; j < 1000; j++) {
                    builder.row(
                            sha256().hashLong(random.nextLong()).toString(),
                            "test" + random.nextInt(100),
                            random.nextLong(100_000),
                            "2018-04-01");
                }
                sink.appendPage(builder.build().toPage());
            }

            // verify we have several temporary files per bucket
            Path stagingPathRoot = getStagingPathRoot(outputHandle);
            HdfsContext context = new HdfsContext(session, table.getSchemaName(), table.getTableName());
            assertThat(listAllDataFiles(context, stagingPathRoot))
                    .filteredOn(file -> file.contains(".tmp-sort."))
                    .size().isGreaterThanOrEqualTo(bucketCount * 3);

            // finish the write
            Collection<Slice> fragments = getFutureValue(sink.finish());

            // verify there are no temporary files
            for (String file : listAllDataFiles(context, stagingPathRoot)) {
                assertThat(file).doesNotStartWith(".tmp-sort.");
            }

            // finish creating table
            metadata.finishCreateTable(session, outputHandle, fragments);

            transaction.commit();
        }

        // verify that bucket files are sorted
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, table);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
            assertThat(splits).hasSize(bucketCount);

            for (ConnectorSplit split : splits) {
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, columnHandles)) {
                    String lastValueAsc = null;
                    long lastValueDesc = -1;

                    while (!pageSource.isFinished()) {
                        Page page = pageSource.getNextPage();
                        if (page == null) {
                            continue;
                        }
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            Block blockAsc = page.getBlock(1);
                            Block blockDesc = page.getBlock(2);
                            assertFalse(blockAsc.isNull(i));
                            assertFalse(blockDesc.isNull(i));

                            String valueAsc = VARCHAR.getSlice(blockAsc, i).toStringUtf8();
                            if (lastValueAsc != null) {
                                assertGreaterThanOrEqual(valueAsc, lastValueAsc);
                                if (valueAsc.equals(lastValueAsc)) {
                                    long valueDesc = BIGINT.getLong(blockDesc, i);
                                    if (lastValueDesc != -1) {
                                        assertLessThanOrEqual(valueDesc, lastValueDesc);
                                    }
                                    lastValueDesc = valueDesc;
                                }
                                else {
                                    lastValueDesc = -1;
                                }
                            }
                            lastValueAsc = valueAsc;
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testInsert()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryInsertTable = temporaryTable("insert");
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
            SchemaTableName temporaryInsertIntoNewPartitionTable = temporaryTable("insert_new_partitioned");
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
            SchemaTableName temporaryInsertIntoExistingPartitionTable = temporaryTable("insert_existing_partitioned");
            try {
                doInsertIntoExistingPartition(storageFormat, temporaryInsertIntoExistingPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoExistingPartitionTable);
            }
        }
    }

    @Test
    public void testInsertIntoExistingPartitionEmptyStatistics()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            SchemaTableName temporaryInsertIntoExistingPartitionTable = temporaryTable("insert_existing_partitioned_empty_statistics");
            try {
                doInsertIntoExistingPartitionEmptyStatistics(storageFormat, temporaryInsertIntoExistingPartitionTable);
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
        SchemaTableName temporaryInsertUnsupportedWriteType = temporaryTable("insert_unsupported_type");
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
            SchemaTableName temporaryMetadataDeleteTable = temporaryTable("metadata_delete");
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
            SchemaTableName temporaryCreateEmptyTable = temporaryTable("create_empty");
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
        SchemaTableName temporaryCreateView = temporaryTable("create_view");
        try {
            verifyViewCreation(temporaryCreateView);
        }
        finally {
            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                metadata.dropView(newSession(), temporaryCreateView);
                transaction.commit();
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

    @Test
    public void testUpdateTableParameters()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("compare_and_set_table_parameters");
        try {
            doCreateEmptyTable(tableName, ORC, CREATE_TABLE_COLUMNS);

            ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
            metastoreClient.updateTableParameters(tableName.getSchemaName(), tableName.getTableName(), parameters -> {
                Map<String, String> updated = new HashMap<>(parameters);
                updated.put("test_key", "test_value");
                return updated;
            });

            Table table = metastoreClient.getTable(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(tableName));
            assertThat(table.getParameters()).contains(entry("test_key", "test_value"));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdatePartitionParameters()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("compare_and_set_partition_parameters");
        try {
            doCreateEmptyTable(tableName, ORC, CREATE_TABLE_COLUMNS_PARTITIONED);
            insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);
            ImmutableList<String> partitionValues = ImmutableList.of("2015-07-01");

            ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
            metastoreClient.updatePartitionParameters(tableName.getSchemaName(), tableName.getTableName(), partitionValues, parameters -> {
                Map<String, String> updated = new HashMap<>(parameters);
                updated.put("test_key", "test_value");
                return updated;
            });

            Partition partition = metastoreClient.getPartition(tableName.getSchemaName(), tableName.getTableName(), partitionValues)
                    .orElseThrow(() -> new PartitionNotFoundException(tableName, partitionValues));
            assertThat(partition.getParameters()).contains(entry("test_key", "test_value"));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateBasicTableStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_basic_table_statistics");
        try {
            doCreateEmptyTable(tableName, ORC, STATISTICS_TABLE_COLUMNS);
            testUpdateTableStatistics(tableName, EMPTY_TABLE_STATISTICS, BASIC_STATISTICS_1, BASIC_STATISTICS_2);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateTableColumnStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_table_column_statistics");

        ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
        if (!metastoreClient.supportsColumnStatistics()) {
            throw new SkipException("column level statistics are not supported");
        }

        try {
            doCreateEmptyTable(tableName, ORC, STATISTICS_TABLE_COLUMNS);
            testUpdateTableStatistics(tableName, EMPTY_TABLE_STATISTICS, STATISTICS_1_1, STATISTICS_1_2, STATISTICS_2);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_table_column_statistics_empty_optional_fields");

        ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
        if (!metastoreClient.supportsColumnStatistics()) {
            throw new SkipException("column level statistics are not supported");
        }

        try {
            doCreateEmptyTable(tableName, ORC, STATISTICS_TABLE_COLUMNS);
            testUpdateTableStatistics(tableName, EMPTY_TABLE_STATISTICS, STATISTICS_EMPTY_OPTIONAL_FIELDS);
        }
        finally {
            dropTable(tableName);
        }
    }

    protected void testUpdateTableStatistics(SchemaTableName tableName, PartitionStatistics initialStatistics, PartitionStatistics... statistics)
    {
        ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
        assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                .isEqualTo(initialStatistics);

        AtomicReference<PartitionStatistics> expectedStatistics = new AtomicReference<>(initialStatistics);
        for (PartitionStatistics partitionStatistics : statistics) {
            metastoreClient.updateTableStatistics(tableName.getSchemaName(), tableName.getTableName(), actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatistics.get());
                return partitionStatistics;
            });
            assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                    .isEqualTo(partitionStatistics);
            expectedStatistics.set(partitionStatistics);
        }

        assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                .isEqualTo(expectedStatistics.get());

        metastoreClient.updateTableStatistics(tableName.getSchemaName(), tableName.getTableName(), actualStatistics -> {
            assertThat(actualStatistics).isEqualTo(expectedStatistics.get());
            return initialStatistics;
        });

        assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                .isEqualTo(initialStatistics);
    }

    @Test
    public void testUpdateBasicPartitionStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_basic_partition_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            testUpdatePartitionStatistics(
                    tableName,
                    EMPTY_TABLE_STATISTICS,
                    ImmutableList.of(BASIC_STATISTICS_1, BASIC_STATISTICS_2),
                    ImmutableList.of(BASIC_STATISTICS_2, BASIC_STATISTICS_1));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdatePartitionColumnStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_partition_column_statistics");

        ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
        if (!metastoreClient.supportsColumnStatistics()) {
            throw new SkipException("column level statistics are not supported");
        }

        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            testUpdatePartitionStatistics(
                    tableName,
                    EMPTY_TABLE_STATISTICS,
                    ImmutableList.of(STATISTICS_1_1, STATISTICS_1_2, STATISTICS_2),
                    ImmutableList.of(STATISTICS_1_2, STATISTICS_1_1, STATISTICS_2));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_partition_column_statistics");

        ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
        if (!metastoreClient.supportsColumnStatistics()) {
            throw new SkipException("column level statistics are not supported");
        }

        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            testUpdatePartitionStatistics(
                    tableName,
                    EMPTY_TABLE_STATISTICS,
                    ImmutableList.of(STATISTICS_EMPTY_OPTIONAL_FIELDS),
                    ImmutableList.of(STATISTICS_EMPTY_OPTIONAL_FIELDS));
        }
        finally {
            dropTable(tableName);
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

    protected void createDummyPartitionedTable(SchemaTableName tableName, List<ColumnMetadata> columns)
            throws Exception
    {
        doCreateEmptyTable(tableName, ORC, columns);

        ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
        Table table = metastoreClient.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        List<String> firstPartitionValues = ImmutableList.of("2016-01-01");
        List<String> secondPartitionValues = ImmutableList.of("2016-01-02");

        String firstPartitionName = makePartName(ImmutableList.of("ds"), firstPartitionValues);
        String secondPartitionName = makePartName(ImmutableList.of("ds"), secondPartitionValues);

        List<Partition> partitions = ImmutableList.of(firstPartitionValues, secondPartitionValues)
                .stream()
                .map(values -> Partition.builder()
                        .setDatabaseName(tableName.getSchemaName())
                        .setTableName(tableName.getTableName())
                        .setColumns(table.getDataColumns())
                        .setValues(values)
                        .withStorage(storage -> storage
                                .setStorageFormat(fromHiveStorageFormat(HiveStorageFormat.ORC))
                                .setLocation(table.getStorage().getLocation() + "/" + makePartName(ImmutableList.of("ds"), values)))
                        .build())
                .collect(toImmutableList());
        metastoreClient.addPartitions(tableName.getSchemaName(), tableName.getTableName(), partitions);
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, currentStatistics -> EMPTY_TABLE_STATISTICS);
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, currentStatistics -> EMPTY_TABLE_STATISTICS);
    }

    protected void testUpdatePartitionStatistics(
            SchemaTableName tableName,
            PartitionStatistics initialStatistics,
            List<PartitionStatistics> firstPartitionStatistics,
            List<PartitionStatistics> secondPartitionStatistics)
    {
        verify(firstPartitionStatistics.size() == secondPartitionStatistics.size());

        String firstPartitionName = "ds=2016-01-01";
        String secondPartitionName = "ds=2016-01-02";

        ExtendedHiveMetastore metastoreClient = getMetastoreClient(tableName.getSchemaName());
        assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                .isEqualTo(ImmutableMap.of(firstPartitionName, initialStatistics, secondPartitionName, initialStatistics));

        AtomicReference<PartitionStatistics> expectedStatisticsPartition1 = new AtomicReference<>(initialStatistics);
        AtomicReference<PartitionStatistics> expectedStatisticsPartition2 = new AtomicReference<>(initialStatistics);

        for (int i = 0; i < firstPartitionStatistics.size(); i++) {
            PartitionStatistics statisticsPartition1 = firstPartitionStatistics.get(i);
            PartitionStatistics statisticsPartition2 = secondPartitionStatistics.get(i);
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatisticsPartition1.get());
                return statisticsPartition1;
            });
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatisticsPartition2.get());
                return statisticsPartition2;
            });
            assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                    .isEqualTo(ImmutableMap.of(firstPartitionName, statisticsPartition1, secondPartitionName, statisticsPartition2));
            expectedStatisticsPartition1.set(statisticsPartition1);
            expectedStatisticsPartition2.set(statisticsPartition2);
        }

        assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                .isEqualTo(ImmutableMap.of(firstPartitionName, expectedStatisticsPartition1.get(), secondPartitionName, expectedStatisticsPartition2.get()));
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, currentStatistics -> {
            assertThat(currentStatistics).isEqualTo(expectedStatisticsPartition1.get());
            return initialStatistics;
        });
        metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, currentStatistics -> {
            assertThat(currentStatistics).isEqualTo(expectedStatisticsPartition2.get());
            return initialStatistics;
        });
        assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                .isEqualTo(ImmutableMap.of(firstPartitionName, initialStatistics, secondPartitionName, initialStatistics));
    }

    private void verifyViewCreation(SchemaTableName temporaryCreateView)
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
            HdfsContext context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            for (String filePath : listAllDataFiles(context, getStagingPathRoot(outputHandle))) {
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
            assertEquals(table.getParameters().get(PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
            assertEquals(table.getParameters().get(PRESTO_QUERY_ID_NAME), queryId);

            // verify basic statistics
            HiveBasicStatistics statistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(statistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount());
            assertEquals(statistics.getFileCount().getAsLong(), 1L);
            assertGreaterThan(statistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
            assertGreaterThan(statistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
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
            metadata.createTable(session, tableMetadata, false);
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
                            column.getComment(),
                            columnExtraInfo(partitionedBy.contains(column.getName())),
                            false))
                    .collect(toList());
            assertEquals(filterNonHiddenColumnMetadata(tableMetadata.getColumns()), expectedColumns);

            // verify table format
            Table table = transaction.getMetastore(tableName.getSchemaName()).getTable(tableName.getSchemaName(), tableName.getTableName()).get();
            assertEquals(table.getStorage().getStorageFormat().getInputFormat(), storageFormat.getInputFormat());

            // verify the node version and query ID
            assertEquals(table.getParameters().get(PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
            assertEquals(table.getParameters().get(PRESTO_QUERY_ID_NAME), queryId);

            // verify the table is empty
            List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());
            MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEquals(result.getRowCount(), 0);

            // verify basic statistics
            if (partitionedBy.isEmpty()) {
                HiveBasicStatistics statistics = getBasicStatisticsForTable(transaction, tableName);
                assertEquals(statistics.getRowCount().getAsLong(), 0L);
                assertEquals(statistics.getFileCount().getAsLong(), 0L);
                assertEquals(statistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                assertEquals(statistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
            }
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
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
                assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

                // statistics
                HiveBasicStatistics tableStatistics = getBasicStatisticsForTable(transaction, tableName);
                assertEquals(tableStatistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * (i + 1));
                assertEquals(tableStatistics.getFileCount().getAsLong(), i + 1L);
                assertGreaterThan(tableStatistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                assertGreaterThan(tableStatistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
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

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            sink.appendPage(CREATE_TABLE_DATA.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());
            metadata.finishInsert(session, insertTableHandle, fragments);

            // statistics, visible from within transaction
            HiveBasicStatistics tableStatistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(tableStatistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * 5L);

            try (Transaction otherTransaction = newTransaction()) {
                // statistics, not visible from outside transaction
                HiveBasicStatistics otherTableStatistics = getBasicStatisticsForTable(otherTransaction, tableName);
                assertEquals(otherTableStatistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * 3L);
            }

            // verify we did not modify the table directory
            assertEquals(listAllDataFiles(transaction, tableName.getSchemaName(), tableName.getTableName()), existingFiles);

            // verify all temp files start with the unique prefix
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            HdfsContext context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            Set<String> tempFiles = listAllDataFiles(context, stagingPathRoot);
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
            }

            // rollback insert
            transaction.rollback();
        }

        // verify temp directory is empty
        HdfsContext context = new HdfsContext(newSession(), tableName.getSchemaName(), tableName.getTableName());
        assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());

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

        // verify statistics unchanged
        try (Transaction transaction = newTransaction()) {
            HiveBasicStatistics statistics = getBasicStatisticsForTable(transaction, tableName);
            assertEquals(statistics.getRowCount().getAsLong(), CREATE_TABLE_DATA.getRowCount() * 3L);
            assertEquals(statistics.getFileCount().getAsLong(), 3L);
        }
    }

    // These are protected so extensions to the hive connector can replace the handle classes
    protected String getFilePrefix(ConnectorOutputTableHandle outputTableHandle)
    {
        return ((HiveWritableTableHandle) outputTableHandle).getFilePrefix();
    }

    protected String getFilePrefix(ConnectorInsertTableHandle insertTableHandle)
    {
        return ((HiveWritableTableHandle) insertTableHandle).getFilePrefix();
    }

    protected Path getStagingPathRoot(ConnectorInsertTableHandle insertTableHandle)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) insertTableHandle;
        WriteInfo writeInfo = getLocationService(handle.getSchemaName()).getQueryWriteInfo(handle.getLocationHandle());
        if (writeInfo.getWriteMode() != STAGE_AND_MOVE_TO_TARGET_DIRECTORY) {
            throw new AssertionError("writeMode is not STAGE_AND_MOVE_TO_TARGET_DIRECTORY");
        }
        return writeInfo.getWritePath();
    }

    protected Path getStagingPathRoot(ConnectorOutputTableHandle outputTableHandle)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) outputTableHandle;
        return getLocationService(handle.getSchemaName())
                .getQueryWriteInfo(handle.getLocationHandle())
                .getWritePath();
    }

    protected Path getTargetPathRoot(ConnectorInsertTableHandle insertTableHandle)
    {
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle;

        return getLocationService(hiveInsertTableHandle.getSchemaName())
                .getQueryWriteInfo(hiveInsertTableHandle.getLocationHandle())
                .getTargetPath();
    }

    protected Set<String> listAllDataFiles(Transaction transaction, String schemaName, String tableName)
            throws IOException
    {
        HdfsContext context = new HdfsContext(newSession(), schemaName, tableName);
        Set<String> existingFiles = new HashSet<>();
        for (String location : listAllDataPaths(transaction.getMetastore(schemaName), schemaName, tableName)) {
            existingFiles.addAll(listAllDataFiles(context, new Path(location)));
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

    protected Set<String> listAllDataFiles(HdfsContext context, Path path)
            throws IOException
    {
        Set<String> result = new HashSet<>();
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, path);
        if (fileSystem.exists(path)) {
            for (FileStatus fileStatus : fileSystem.listStatus(path)) {
                if (fileStatus.getPath().getName().startsWith(".presto")) {
                    // skip hidden files
                }
                else if (fileStatus.isFile()) {
                    result.add(fileStatus.getPath().toString());
                }
                else if (fileStatus.isDirectory()) {
                    result.addAll(listAllDataFiles(context, fileStatus.getPath()));
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
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toList()));

            // verify the node versions in partitions
            Map<String, Optional<Partition>> partitions = getMetastoreClient(tableName.getSchemaName()).getPartitionsByNames(tableName.getSchemaName(), tableName.getTableName(), partitionNames);
            assertEquals(partitions.size(), partitionNames.size());
            for (String partitionName : partitionNames) {
                Partition partition = partitions.get(partitionName).get();
                assertEquals(partition.getParameters().get(PRESTO_VERSION_NAME), TEST_SERVER_VERSION);
                assertEquals(partition.getParameters().get(PRESTO_QUERY_ID_NAME), queryId);
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

            // test statistics
            for (String partitionName : partitionNames) {
                HiveBasicStatistics partitionStatistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertEquals(partitionStatistics.getRowCount().getAsLong(), 1L);
                assertEquals(partitionStatistics.getFileCount().getAsLong(), 1L);
                assertGreaterThan(partitionStatistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                assertGreaterThan(partitionStatistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
            }
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
            Collection<Slice> fragments = getFutureValue(sink.finish());
            metadata.finishInsert(session, insertTableHandle, fragments);

            // verify all temp files start with the unique prefix
            HdfsContext context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            Set<String> tempFiles = listAllDataFiles(context, getStagingPathRoot(insertTableHandle));
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
            HdfsContext context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());
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
            assertThat(e).hasMessageMatching("Inserting into Hive table .* with column type uniontype<smallint,tinyint> not supported");
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
                        .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
                assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                        .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                        .collect(toList()));

                // load the new table
                List<ColumnHandle> columnHandles = filterNonHiddenColumnHandles(metadata.getColumnHandles(session, tableHandle).values());

                // verify the data
                resultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());
                MaterializedResult result = readTable(transaction, tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
                assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

                // test statistics
                for (String partitionName : partitionNames) {
                    HiveBasicStatistics statistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                    assertEquals(statistics.getRowCount().getAsLong(), i + 1L);
                    assertEquals(statistics.getFileCount().getAsLong(), i + 1L);
                    assertGreaterThan(statistics.getInMemoryDataSizeInBytes().getAsLong(), 0L);
                    assertGreaterThan(statistics.getOnDiskDataSizeInBytes().getAsLong(), 0L);
                }
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

            // "stage" insert data
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            stagingPathRoot = getStagingPathRoot(insertTableHandle);
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction.getTransactionHandle(), session, insertTableHandle);
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage());
            sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage());
            Collection<Slice> fragments = getFutureValue(sink.finish());
            metadata.finishInsert(session, insertTableHandle, fragments);

            // verify all temp files start with the unique prefix
            HdfsContext context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            Set<String> tempFiles = listAllDataFiles(context, getStagingPathRoot(insertTableHandle));
            assertTrue(!tempFiles.isEmpty());
            for (String filePath : tempFiles) {
                assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
            }

            // verify statistics are visible from within of the current transaction
            List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            for (String partitionName : partitionNames) {
                HiveBasicStatistics partitionStatistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertEquals(partitionStatistics.getRowCount().getAsLong(), 5L);
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
            HdfsContext context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            assertTrue(listAllDataFiles(context, stagingPathRoot).isEmpty());

            // verify statistics have been rolled back
            List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
            for (String partitionName : partitionNames) {
                HiveBasicStatistics partitionStatistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertEquals(partitionStatistics.getRowCount().getAsLong(), 3L);
            }
        }
    }

    private void doInsertIntoExistingPartitionEmptyStatistics(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
        insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        eraseStatistics(tableName);

        insertData(tableName, CREATE_TABLE_PARTITIONED_DATA);

        try (Transaction transaction = newTransaction()) {
            List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));

            for (String partitionName : partitionNames) {
                HiveBasicStatistics statistics = getBasicStatisticsForPartition(transaction, tableName, partitionName);
                assertThat(statistics.getRowCount()).isNotPresent();
                assertThat(statistics.getInMemoryDataSizeInBytes()).isNotPresent();
                // fileCount and rawSize statistics are computed on the fly by the metastore, thus cannot be erased
            }
        }
    }

    private static HiveBasicStatistics getBasicStatisticsForTable(Transaction transaction, SchemaTableName table)
    {
        return transaction
                .getMetastore(table.getSchemaName())
                .getTableStatistics(table.getSchemaName(), table.getTableName())
                .getBasicStatistics();
    }

    private static HiveBasicStatistics getBasicStatisticsForPartition(Transaction transaction, SchemaTableName table, String partitionName)
    {
        return transaction
                .getMetastore(table.getSchemaName())
                .getPartitionStatistics(table.getSchemaName(), table.getTableName(), ImmutableSet.of(partitionName))
                .get(partitionName)
                .getBasicStatistics();
    }

    private void eraseStatistics(SchemaTableName schemaTableName)
    {
        ExtendedHiveMetastore metastoreClient = getMetastoreClient(schemaTableName.getSchemaName());
        metastoreClient.updateTableStatistics(schemaTableName.getSchemaName(), schemaTableName.getTableName(), statistics -> new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));
        Table table = metastoreClient.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        List<String> partitionColumns = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        if (!table.getPartitionColumns().isEmpty()) {
            List<String> partitionNames = metastoreClient.getPartitionNames(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                    .orElse(ImmutableList.of());
            List<Partition> partitions = metastoreClient
                    .getPartitionsByNames(schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionNames)
                    .entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
            for (Partition partition : partitions) {
                metastoreClient.updatePartitionStatistics(
                        schemaTableName.getSchemaName(),
                        schemaTableName.getTableName(),
                        makePartName(partitionColumns, partition.getValues()),
                        statistics -> new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));
            }
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
            HdfsContext context = new HdfsContext(newSession(), tableName.getSchemaName(), tableName.getTableName());
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, writePath);
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
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
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
            ConnectorTableLayoutHandle tableLayoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
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
                    .collect(toImmutableList());
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
            ConnectorTableLayoutHandle tableLayoutHandle2 = getOnlyElement(tableLayoutResults2).getTableLayout().getHandle();
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
    {
        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 1);
        return (HiveSplit) getOnlyElement(splits);
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
                        SqlTimestamp expected = sqlTimestampOf(2011, 5, 6, 7, 8, 9, 123, timeZone, UTC_KEY, SESSION);
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
                        SqlDate expected = new SqlDate(toIntExact(MILLISECONDS.toDays(new DateTime(2013, 8, 9, 0, 0, 0, UTC).getMillis())));
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
                        List<Object> expected1 = ImmutableList.of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.of("test xyz", 0.2);
                        assertEquals(row.getField(index), ImmutableList.of(expected1, expected2));
                    }
                }

                // STRUCT<s_string: STRING, s_double:DOUBLE>
                index = columnIndex.get("t_struct");
                if (index != null) {
                    if ((rowNumber % 31) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertTrue(row.getField(index) instanceof List);
                        List values = (List) row.getField(index);
                        assertEquals(values.size(), 2);
                        assertEquals(values.get(0), "test abc");
                        assertEquals(values.get(1), 0.1);
                    }
                }

                // MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
                index = columnIndex.get("t_complex");
                if (index != null) {
                    if ((rowNumber % 33) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        List<Object> expected1 = ImmutableList.of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.of("test xyz", 0.2);
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
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle, UNGROUPED_SCHEDULING));
        if (expectedSplitCount.isPresent()) {
            assertEquals(splits.size(), expectedSplitCount.getAsInt());
        }

        ImmutableList.Builder<MaterializedRow> allRows = ImmutableList.builder();
        for (ConnectorSplit split : splits) {
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction.getTransactionHandle(), session, split, columnHandles)) {
                expectedStorageFormat.ifPresent(format -> assertPageSourceType(pageSource, format));
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
    {
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            splitCount += getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits().size();
        }
        return splitCount;
    }

    private List<ConnectorSplit> getAllSplits(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = transaction.getMetadata();
            List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
            ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
            return getAllSplits(splitManager.getSplits(transaction.getTransactionHandle(), session, layoutHandle, UNGROUPED_SCHEDULING));
        }
    }

    protected static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits());
        }
        return splits.build();
    }

    protected List<?> getAllPartitions(ConnectorTableLayoutHandle layoutHandle)
    {
        return ((HiveTableLayoutHandle) layoutHandle).getPartitions()
                .orElseThrow(() -> new AssertionError("layout has no partitions"));
    }

    protected String getPartitionId(Object partition)
    {
        return ((HivePartition) partition).getPartitionId();
    }

    protected static void assertPageSourceType(ConnectorPageSource pageSource, HiveStorageFormat hiveStorageFormat)
    {
        if (pageSource instanceof RecordPageSource) {
            RecordCursor hiveRecordCursor = ((RecordPageSource) pageSource).getCursor();
            hiveRecordCursor = ((HiveRecordCursor) hiveRecordCursor).getRegularColumnRecordCursor();
            if (hiveRecordCursor instanceof HiveCoercionRecordCursor) {
                hiveRecordCursor = ((HiveCoercionRecordCursor) hiveRecordCursor).getRegularColumnRecordCursor();
            }
            assertInstanceOf(hiveRecordCursor, recordCursorType(hiveStorageFormat), hiveStorageFormat.name());
        }
        else {
            assertInstanceOf(((HivePageSource) pageSource).getPageSource(), pageSourceType(hiveStorageFormat), hiveStorageFormat.name());
        }
    }

    private static Class<? extends RecordCursor> recordCursorType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
            case PARQUET:
                return ParquetHiveRecordCursor.class;
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
            case PARQUET:
                return ParquetPageSource.class;
            default:
                throw new AssertionError("File type does not use a PageSource: " + hiveStorageFormat);
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
                else if (column.getType() instanceof ArrayType || column.getType() instanceof RowType) {
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
        assertEquals(column.getExtraInfo(), columnExtraInfo(partitionKey));
    }

    protected static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;
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

    protected SchemaTableName temporaryTable(String tableName)
    {
        return temporaryTable(database, tableName);
    }

    protected static SchemaTableName temporaryTable(String database, String tableName)
    {
        String randomName = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return new SchemaTableName(database, TEMPORARY_TABLE_PREFIX + tableName + "_" + randomName);
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
                .put(SORTED_BY_PROPERTY, ImmutableList.of())
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

    private void createEmptyTable(SchemaTableName schemaTableName, HiveStorageFormat hiveStorageFormat, List<Column> columns, List<Column> partitionColumns)
            throws Exception
    {
        createEmptyTable(schemaTableName, hiveStorageFormat, columns, partitionColumns, Optional.empty());
    }

    private void createEmptyTable(SchemaTableName schemaTableName, HiveStorageFormat hiveStorageFormat, List<Column> columns, List<Column> partitionColumns, Optional<HiveBucketProperty> bucketProperty)
            throws Exception
    {
        Path targetPath;

        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();

            String tableOwner = session.getUser();
            String schemaName = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();

            LocationService locationService = getLocationService(schemaName);
            LocationHandle locationHandle = locationService.forNewTable(transaction.getMetastore(schemaName), session, schemaName, tableName);
            targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();

            Table.Builder tableBuilder = Table.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setOwner(tableOwner)
                    .setTableType(TableType.MANAGED_TABLE.name())
                    .setParameters(ImmutableMap.of(
                            PRESTO_VERSION_NAME, TEST_SERVER_VERSION,
                            PRESTO_QUERY_ID_NAME, session.getQueryId()))
                    .setDataColumns(columns)
                    .setPartitionColumns(partitionColumns);

            tableBuilder.getStorageBuilder()
                    .setLocation(targetPath.toString())
                    .setStorageFormat(StorageFormat.create(hiveStorageFormat.getSerDe(), hiveStorageFormat.getInputFormat(), hiveStorageFormat.getOutputFormat()))
                    .setBucketProperty(bucketProperty)
                    .setSerdeParameters(ImmutableMap.of());

            PrincipalPrivileges principalPrivileges = testingPrincipalPrivilege(tableOwner);
            transaction.getMetastore(schemaName).createTable(session, tableBuilder.build(), principalPrivileges, Optional.empty(), true, EMPTY_TABLE_STATISTICS);

            transaction.commit();
        }

        HdfsContext context = new HdfsContext(newSession(), schemaTableName.getSchemaName(), schemaTableName.getTableName());
        List<String> targetDirectoryList = listDirectory(context, targetPath);
        assertEquals(targetDirectoryList, ImmutableList.of());
    }

    private void alterBucketProperty(SchemaTableName schemaTableName, Optional<HiveBucketProperty> bucketProperty)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorSession session = newSession();

            String tableOwner = session.getUser();
            String schemaName = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();

            Optional<Table> table = transaction.getMetastore(schemaName).getTable(schemaName, tableName);
            Table.Builder tableBuilder = Table.builder(table.get());
            tableBuilder.getStorageBuilder().setBucketProperty(bucketProperty);
            PrincipalPrivileges principalPrivileges = testingPrincipalPrivilege(tableOwner);
            // hack: replaceView can be used as replaceTable despite its name
            transaction.getMetastore(schemaName).replaceView(schemaName, tableName, tableBuilder.build(), principalPrivileges);

            transaction.commit();
        }
    }

    private PrincipalPrivileges testingPrincipalPrivilege(String tableOwner)
    {
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.SELECT, true))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.INSERT, true))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.UPDATE, true))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.DELETE, true))
                        .build(),
                ImmutableMultimap.of());
    }

    private List<String> listDirectory(HdfsContext context, Path path)
            throws IOException
    {
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, path);
        return Arrays.stream(fileSystem.listStatus(path))
                .map(FileStatus::getPath)
                .map(Path::getName)
                .filter(name -> !name.startsWith(".presto"))
                .collect(toList());
    }

    @Test
    public void testTransactionDeleteInsert()
            throws Exception
    {
        doTestTransactionDeleteInsert(
                RCBINARY,
                true,
                ImmutableList.<TransactionDeleteInsertTestCase>builder()
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_RIGHT_AWAY, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_DELETE, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_BEGIN_INSERT, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_APPEND_PAGE, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_SINK_FINISH, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, ROLLBACK_AFTER_FINISH_INSERT, Optional.empty()))
                        .add(new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new AddPartitionFailure())))
                        .add(new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new DirectoryRenameFailure())))
                        .add(new TransactionDeleteInsertTestCase(false, false, COMMIT, Optional.of(new FileRenameFailure())))
                        .add(new TransactionDeleteInsertTestCase(true, false, COMMIT, Optional.of(new DropPartitionFailure())))
                        .add(new TransactionDeleteInsertTestCase(true, true, COMMIT, Optional.empty()))
                        .build());
    }

    protected void doTestTransactionDeleteInsert(HiveStorageFormat storageFormat, boolean allowInsertExisting, List<TransactionDeleteInsertTestCase> testCases)
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

        for (TransactionDeleteInsertTestCase testCase : testCases) {
            SchemaTableName temporaryDeleteInsert = temporaryTable("delete_insert");
            try {
                createEmptyTable(
                        temporaryDeleteInsert,
                        storageFormat,
                        ImmutableList.of(new Column("col1", HIVE_LONG, Optional.empty())),
                        ImmutableList.of(new Column("pk1", HIVE_STRING, Optional.empty()), new Column("pk2", HIVE_STRING, Optional.empty())));
                insertData(temporaryDeleteInsert, beforeData);
                try {
                    doTestTransactionDeleteInsert(
                            storageFormat,
                            temporaryDeleteInsert,
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
            }
            finally {
                dropTable(temporaryDeleteInsert);
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
                ConnectorTableLayoutHandle tableLayoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
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
            HdfsContext context = new HdfsContext(newSession(), tableName.getSchemaName(), tableName.getTableName());
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, writePath);
            assertFalse(fileSystem.exists(writePath));
        }

        try (Transaction transaction = newTransaction()) {
            // verify partitions
            List<String> partitionNames = transaction.getMetastore(tableName.getSchemaName())
                    .getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new AssertionError("Table does not exist: " + tableName));
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

    private static void rollbackIfEquals(TransactionDeleteInsertTestTag tag, TransactionDeleteInsertTestTag expectedTag)
    {
        if (expectedTag == tag) {
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
        private HdfsContext context;
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
            context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            createDirectory(context, hdfsEnvironment, path);
        }

        @Override
        public void verifyAndCleanup(SchemaTableName tableName)
                throws IOException
        {
            assertEquals(listDirectory(context, path), ImmutableList.of());
            hdfsEnvironment.getFileSystem(context, path).delete(path, false);
        }
    }

    protected class FileRenameFailure
            implements ConflictTrigger
    {
        private HdfsContext context;
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

            context = new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName());
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, path);
            fileSystem.createNewFile(path);
        }

        @Override
        public void verifyAndCleanup(SchemaTableName tableName)
                throws IOException
        {
            // The file we added to trigger a conflict was cleaned up because it matches the query prefix.
            // Consider this the same as a network failure that caused the successful creation of file not reported to the caller.
            assertFalse(hdfsEnvironment.getFileSystem(context, path).exists(path));
        }
    }
}
