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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveCommonSessionProperties.PARQUET_BATCH_READ_OPTIMIZATION_ENABLED;
import static com.facebook.presto.iceberg.FileContent.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.IcebergSessionProperties.DELETE_AS_JOIN_REWRITE_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class IcebergDistributedTestBase
        extends AbstractTestQueryFramework
{
    private static final String METADATA_FILE_EXTENSION = ".metadata.json";
    private final CatalogType catalogType;
    private final Map<String, String> extraConnectorProperties;

    protected IcebergDistributedTestBase(CatalogType catalogType, Map<String, String> extraConnectorProperties)
    {
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
        this.extraConnectorProperties = requireNonNull(extraConnectorProperties, "extraConnectorProperties is null");
    }

    protected IcebergDistributedTestBase(CatalogType catalogType)
    {
        this(catalogType, ImmutableMap.of());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), catalogType, extraConnectorProperties);
    }

    @Test
    public void testDeleteOnV1Table()
    {
        // Test delete all rows
        long totalCount = (long) getQueryRunner().execute("CREATE TABLE test_delete with (format_version = '1') as select * from lineitem")
                .getOnlyValue();
        assertUpdate("DELETE FROM test_delete", totalCount);
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 0L);
        assertQuerySucceeds("DROP TABLE test_delete");

        // Test delete whole partitions identified by one partition column
        totalCount = (long) getQueryRunner().execute("CREATE TABLE test_partitioned_drop WITH (format_version = '1', partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem")
                .getOnlyValue();
        long countPart1 = (long) getQueryRunner().execute("SELECT count(*) FROM test_partitioned_drop where linenumber = 1").getOnlyValue();
        assertUpdate("DELETE FROM test_partitioned_drop WHERE linenumber = 1", countPart1);

        long countPart2 = (long) getQueryRunner().execute("SELECT count(*) FROM test_partitioned_drop where linenumber > 4 and linenumber < 7").getOnlyValue();
        assertUpdate("DELETE FROM test_partitioned_drop WHERE linenumber > 4 and linenumber < 7", countPart2);

        long newTotalCount = (long) getQueryRunner().execute("SELECT count(*) FROM test_partitioned_drop")
                .getOnlyValue();
        assertEquals(totalCount - countPart1 - countPart2, newTotalCount);
        assertQuerySucceeds("DROP TABLE test_partitioned_drop");

        // Test delete whole partitions identified by two partition columns
        totalCount = (long) getQueryRunner().execute("CREATE TABLE test_partitioned_drop WITH (format_version = '1', partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem")
                .getOnlyValue();
        long countPart1F = (long) getQueryRunner().execute("SELECT count(*) FROM test_partitioned_drop where linenumber = 1 and linestatus = 'F'").getOnlyValue();
        assertUpdate("DELETE FROM test_partitioned_drop WHERE linenumber = 1 and linestatus = 'F'", countPart1F);

        long countPart2O = (long) getQueryRunner().execute("SELECT count(*) FROM test_partitioned_drop where linenumber = 2 and linestatus = 'O'").getOnlyValue();
        assertUpdate("DELETE FROM test_partitioned_drop WHERE linenumber = 2 and linestatus = 'O'", countPart2O);

        long countPartOther = (long) getQueryRunner().execute("SELECT count(*) FROM test_partitioned_drop where linenumber not in (1, 3, 5, 7) and linestatus in ('O', 'F')").getOnlyValue();
        assertUpdate("DELETE FROM test_partitioned_drop WHERE linenumber not in (1, 3, 5, 7) and linestatus in ('O', 'F')", countPartOther);

        newTotalCount = (long) getQueryRunner().execute("SELECT count(*) FROM test_partitioned_drop")
                .getOnlyValue();
        assertEquals(totalCount - countPart1F - countPart2O - countPartOther, newTotalCount);
        assertQuerySucceeds("DROP TABLE test_partitioned_drop");

        String errorMessage1 = "This connector only supports delete where one or more partitions are deleted entirely for table versions older than 2";
        // Do not support delete with filters on non-identity partition column on v1 table
        assertUpdate("CREATE TABLE test_partitioned_drop WITH (format_version = '1', partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem", totalCount);
        assertQueryFails("DELETE FROM test_partitioned_drop WHERE orderkey = 1", errorMessage1);

        // Do not allow delete data at specified snapshot
        String errorMessage2 = "This connector do not allow delete data at specified snapshot";
        List<Long> snapshots = getQueryRunner().execute("SELECT snapshot_id FROM \"test_partitioned_drop$snapshots\"").getOnlyColumnAsSet()
                .stream().map(Long.class::cast).collect(Collectors.toList());
        for (long snapshot : snapshots) {
            assertQueryFails("DELETE FROM \"test_partitioned_drop@" + snapshot + "\" WHERE linenumber = 1", errorMessage2);
        }

        assertQuerySucceeds("DROP TABLE test_partitioned_drop");
    }

    @Test
    public void testDeleteWithPartitionSpecEvolution()
    {
        // Create a partitioned table and insert some value
        assertQuerySucceeds("create table test_delete(a int, b varchar) with (partitioning = ARRAY['a'])");
        assertQuerySucceeds("insert into test_delete values(1, '1001'), (1, '1002'), (2, '1003')");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 3L);

        // Rename partition column would not affect metadata deletion filtering by it's value
        assertQuerySucceeds("alter table test_delete rename column a to c");
        assertQuerySucceeds("insert into test_delete(c, b) values(2, '1004')");
        assertUpdate("DELETE FROM test_delete WHERE c = 2", 2L);
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 2L);

        // Add a new partition column, and insert some value
        assertQuerySucceeds("alter table test_delete add column d bigint with(partitioning = 'identity')");
        assertQuerySucceeds("insert into test_delete values(1, '1001', 10001), (2, '1003', 10002), (3, '1004', 10003)");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 5L);

        // Deletion succeeds for merge-on-read mode even though column 'd' does not exist in older partition specs
        assertUpdate("delete from test_delete where d = 10003", 1);
        assertUpdate("delete from test_delete where d = 10002 and c = 2", 1);

        // Deletion succeeds, because column 'c' exists in all partition specs
        assertUpdate("DELETE FROM test_delete WHERE c = 1", 3);
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 0L);

        assertQuerySucceeds("DROP TABLE test_delete");
    }

    @Test
    public void testRenamePartitionColumn()
    {
        assertQuerySucceeds("create table test_partitioned_table(a int, b varchar) with (partitioning = ARRAY['a'])");
        assertQuerySucceeds("insert into test_partitioned_table values(1, '1001'), (2, '1002')");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM \"test_partitioned_table$partitions\"").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("SELECT row_count FROM \"test_partitioned_table$partitions\" where a = 1").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("SELECT row_count FROM \"test_partitioned_table$partitions\" where a = 2").getOnlyValue(), 1L);

        assertQuerySucceeds("alter table test_partitioned_table rename column a to c");
        assertQuerySucceeds("insert into test_partitioned_table values(1, '5001'), (2, '5002'), (3, '5003')");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM \"test_partitioned_table$partitions\"").getOnlyValue(), 3L);
        assertEquals(getQueryRunner().execute("SELECT row_count FROM \"test_partitioned_table$partitions\" where c = 1").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("SELECT row_count FROM \"test_partitioned_table$partitions\" where c = 2").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("SELECT row_count FROM \"test_partitioned_table$partitions\" where c = 3").getOnlyValue(), 1L);
        assertQuerySucceeds("DROP TABLE test_partitioned_table");
    }

    @Test
    public void testAddPartitionColumn()
    {
        // Create unPartitioned table and insert some data
        assertQuerySucceeds("create table add_partition_column(a int)");
        assertQuerySucceeds("insert into add_partition_column values 1, 2, 3");

        // Add identity partition column
        assertQuerySucceeds("alter table add_partition_column add column b timestamp with(partitioning = 'identity')");
        assertQuerySucceeds("insert into add_partition_column values (4, timestamp '1984-12-08 00:10:00'), (5, timestamp '2001-01-08 12:10:01')");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 3L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b is null").getOnlyValue(), 3L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '2001-01-08 12:10:01'").getOnlyValue(), 1L);

        // Add truncate[2] partition column
        assertQuerySucceeds("alter table add_partition_column add column c varchar with(partitioning = 'truncate(2)')");
        assertQuerySucceeds("insert into add_partition_column values (6, timestamp '1984-12-08 00:10:00', 'hw1006'), (7, timestamp '1984-12-08 00:10:00', 'hw1007')");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 4L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b is null and c_trunc is null").getOnlyValue(), 3L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '1984-12-08 00:10:00' and c_trunc is null").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '2001-01-08 12:10:01' and c_trunc is null").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '1984-12-08 00:10:00' and c_trunc = 'hw'").getOnlyValue(), 2L);

        assertQuerySucceeds("DROP TABLE add_partition_column");

        // Create partitioned table and insert some data
        assertQuerySucceeds("create table add_partition_column(a int, b timestamp, c varchar) with (partitioning = ARRAY['b', 'truncate(c, 2)'])");
        assertQuerySucceeds("insert into add_partition_column values(1, timestamp '1984-12-08 00:10:00', 'hw1001'), (2, timestamp '2001-01-08 12:10:01', 'hw1002')");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '1984-12-08 00:10:00' and c_trunc = 'hw'").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '2001-01-08 12:10:01' and c_trunc = 'hw'").getOnlyValue(), 1L);

        // Add bucket partition column
        assertQuerySucceeds("alter table add_partition_column add column d bigint with(partitioning = 'bucket(2)')");
        assertQuerySucceeds("insert into add_partition_column values(1, timestamp '1984-12-08 00:10:00', 'hw1001', 1234), (1, timestamp '1984-12-08 00:10:00', 'hw1001', 1344)");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 4L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '1984-12-08 00:10:00' and c_trunc = 'hw' and d_bucket is null").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '2001-01-08 12:10:01' and c_trunc = 'hw' and d_bucket is null").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '1984-12-08 00:10:00' and c_trunc = 'hw' and d_bucket = 1").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"add_partition_column$partitions\" where b = timestamp '1984-12-08 00:10:00' and c_trunc = 'hw' and d_bucket = 0").getOnlyValue(), 1L);

        assertQuerySucceeds("DROP TABLE add_partition_column");

        Session session = Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
        // Create partitioned table and insert some data
        assertQuerySucceeds(session, "create table add_partition_column(a int, b varchar) with (partitioning = ARRAY['truncate(b, 2)'])");
        assertQuerySucceeds(session, "insert into add_partition_column values(1, 'hw1001'), (2, 'hw1002')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute(session, "select row_count from \"add_partition_column$partitions\" where b_trunc = 'hw'").getOnlyValue(), 2L);

        // Add year partition column
        assertQuerySucceeds(session, "alter table add_partition_column add column c date with(partitioning = 'year')");
        assertQuerySucceeds(session, "insert into add_partition_column values(3, 'hw1003', date '1984-12-08'), (4, 'hw1004', date '2001-01-08')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 3L);

        assertQuery(session, "SELECT b_trunc, c_year, row_count FROM \"add_partition_column$partitions\" ORDER BY c_year",
                "VALUES ('hw', 14, 1), ('hw', 31, 1), ('hw', NULL, 2)");
        assertQuery(session, "SELECT * FROM add_partition_column WHERE year(c) = 1984", "VALUES (3, 'hw1003', '1984-12-08')");

        assertQuerySucceeds(session, "DROP TABLE add_partition_column");

        // Create partitioned table and insert some data
        assertQuerySucceeds(session, "create table add_partition_column(a int, b varchar) with (partitioning = ARRAY['truncate(b, 2)'])");
        assertQuerySucceeds(session, "insert into add_partition_column values(1, 'hw1001'), (2, 'hw1002')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute(session, "select row_count from \"add_partition_column$partitions\" where b_trunc = 'hw'").getOnlyValue(), 2L);

        // Add month partition column
        assertQuerySucceeds(session, "alter table add_partition_column add column c timestamp with(partitioning = 'month')");
        assertQuerySucceeds(session, "insert into add_partition_column values(3, 'hw1003', timestamp '1984-12-08 12:10:31.315'), (4, 'hw1004', timestamp '2001-01-08 10:01:01.123')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 3L);

        assertQuery(session, "SELECT b_trunc, c_month, row_count FROM \"add_partition_column$partitions\" ORDER BY c_month",
                "VALUES ('hw', 179, 1), ('hw', 372, 1), ('hw', NULL, 2)");
        assertQuery(session, "SELECT * FROM add_partition_column WHERE month(c) = 12", "VALUES (3, 'hw1003', '1984-12-08 12:10:31.315')");

        assertQuerySucceeds(session, "DROP TABLE add_partition_column");

        // Create partitioned table and insert some data
        assertQuerySucceeds(session, "create table add_partition_column(a int, b varchar) with (partitioning = ARRAY['truncate(b, 2)'])");
        assertQuerySucceeds(session, "insert into add_partition_column values(1, 'hw1001'), (2, 'hw1002')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute(session, "select row_count from \"add_partition_column$partitions\" where b_trunc = 'hw'").getOnlyValue(), 2L);

        // Add day partition column
        assertQuerySucceeds(session, "alter table add_partition_column add column c date with(partitioning = 'day')");
        assertQuerySucceeds(session, "insert into add_partition_column values(3, 'hw1003', date '1984-12-08'), (4, 'hw1004', date '2001-01-09')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 3L);

        assertQuerySucceeds(session, "DROP TABLE add_partition_column");

        // Create partitioned table and insert some data
        assertQuerySucceeds(session, "create table add_partition_column(a int, b varchar) with (partitioning = ARRAY['truncate(b, 2)'])");
        assertQuerySucceeds(session, "insert into add_partition_column values(1, 'hw1001'), (2, 'hw1002')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute(session, "select row_count from \"add_partition_column$partitions\" where b_trunc = 'hw'").getOnlyValue(), 2L);

        // Add hour partition column
        assertQuerySucceeds(session, "alter table add_partition_column add column c timestamp with(partitioning = 'hour')");
        assertQuerySucceeds(session, "insert into add_partition_column values(3, 'hw1003', timestamp '1984-12-08 12:10:31.315'), (4, 'hw1004', timestamp '2001-01-08 10:01:01.123')");
        assertEquals(getQueryRunner().execute(session, "SELECT count(*) FROM \"add_partition_column$partitions\"").getOnlyValue(), 3L);

        assertQuery(session, "SELECT b_trunc, c_hour, row_count FROM \"add_partition_column$partitions\" ORDER BY c_hour",
                "VALUES ('hw', 130932, 1), ('hw', 271930, 1), ('hw', NULL, 2)");
        assertQuery(session, "SELECT * FROM add_partition_column WHERE hour(c) = 12", "VALUES (3, 'hw1003', '1984-12-08 12:10:31.315')");

        // validate hour() unsupported for date type
        assertQueryFails("alter table add_partition_column add column e date with(partitioning = 'hour')", ".*hour cannot transform date values from.*");

        // validate unknown partition transform, take 'other' and 'null' for example.
        assertQueryFails("alter table add_partition_column add column e varchar with(partitioning = 'other')", "Unknown partition transform: other");
        assertQueryFails("alter table add_partition_column add column e varchar with(partitioning = 'null')", "Unknown partition transform: null");

        // validate wrong parameter of transform function
        String ignoreErrorMessage = ".*";
        long maxValue = Integer.MAX_VALUE + 1;
        assertQueryFails("alter table add_partition_column add column e bigint with (partitioning = 'bucket(-1)')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column e bigint with (partitioning = 'bucket(0)')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column e bigint with (partitioning = 'bucket(1d2f)')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column e bigint with (partitioning = 'bucket(" + maxValue + ")')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column f varchar with (partitioning = 'truncate(-1)')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column f varchar with (partitioning = 'truncate(0)')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column f varchar with (partitioning = 'truncate(1d2f)')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column f varchar with (partitioning = 'truncate(" + maxValue + ")')", ignoreErrorMessage);
        assertQuerySucceeds("DROP TABLE add_partition_column");

        // validate try to add duplicate name in partition spec
        assertQueryFails("create table add_partition_column(a_bucket int, a varchar) with (partitioning = ARRAY['a', 'bucket(a, 2)'])", ignoreErrorMessage);
        assertQuerySucceeds("create table add_partition_column(a int, b_trunc varchar) with (partitioning = ARRAY['bucket(a, 2)', 'b_trunc'])");
        assertQueryFails("alter table add_partition_column add column a_bucket bigint with (partitioning = 'identity')", ignoreErrorMessage);
        assertQueryFails("alter table add_partition_column add column b varchar with (partitioning = 'truncate(2)')", ignoreErrorMessage);
        assertQuerySucceeds("DROP TABLE add_partition_column");
    }

    @Test
    public void testDropPartitionColumn()
    {
        assertQuerySucceeds("create table test_drop_partition_column(a int, b varchar) with (partitioning = ARRAY['a'])");
        assertQuerySucceeds("insert into test_drop_partition_column values(1, '1001'), (2, '1002'), (3, '1003')");
        String errorMessage = "This connector does not support dropping columns which exist in any of the table's partition specs";
        assertQueryFails("alter table test_drop_partition_column drop column a", errorMessage);
        assertQuerySucceeds("DROP TABLE test_drop_partition_column");

        assertQuerySucceeds("create table test_drop_partition_column(a int)");
        assertQuerySucceeds("insert into test_drop_partition_column values 1, 2, 3");
        assertQuerySucceeds("alter table test_drop_partition_column add column b varchar with (partitioning = 'identity')");
        assertQuerySucceeds("insert into test_drop_partition_column values(4, '1004'), (5, '1005')");
        assertQueryFails("alter table test_drop_partition_column drop column b", errorMessage);
        assertQuerySucceeds("DROP TABLE test_drop_partition_column");
    }

    @Test
    public void testTruncate()
    {
        // Test truncate empty table
        assertUpdate("CREATE TABLE test_truncate_empty (i int)");
        assertQuerySucceeds("TRUNCATE TABLE test_truncate_empty");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_truncate_empty").getOnlyValue(), 0L);
        assertQuerySucceeds("DROP TABLE test_truncate_empty");

        // Test truncate table with rows
        assertUpdate("CREATE TABLE test_truncate AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertQuerySucceeds("TRUNCATE TABLE test_truncate");
        MaterializedResult result = getQueryRunner().execute("SELECT count(*) FROM test_truncate");
        assertEquals(result.getOnlyValue(), 0L);
        assertUpdate("DROP TABLE test_truncate");

        // test truncate -> insert -> truncate
        assertUpdate("CREATE TABLE test_truncate_empty (i int)");
        assertQuerySucceeds("TRUNCATE TABLE test_truncate_empty");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_truncate_empty").getOnlyValue(), 0L);
        assertUpdate("INSERT INTO test_truncate_empty VALUES 1", 1);
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_truncate_empty").getOnlyValue(), 1L);
        assertQuerySucceeds("TRUNCATE TABLE test_truncate_empty");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_truncate_empty").getOnlyValue(), 0L);
        assertQuerySucceeds("DROP TABLE test_truncate_empty");

        // Test truncate access control
        assertUpdate("CREATE TABLE test_truncate AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertAccessAllowed("TRUNCATE TABLE test_truncate", privilege("orders", SELECT_COLUMN));
        assertUpdate("DROP TABLE test_truncate");
    }

    @Test
    public void testTruncateTableWithDeleteFiles()
    {
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (format_version = '2', delete_mode = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002'), (3, '1003')", 3);

            // execute row level deletion
            assertUpdate("DELETE FROM " + tableName + " WHERE b in ('1002', '1003')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, '1001')");

            Table icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 1);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 1);

            // execute truncate table
            assertUpdate("TRUNCATE TABLE " + tableName);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");

            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 0);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testShowColumnsForPartitionedTable()
    {
        // Partitioned Table with only identity partitions
        getQueryRunner().execute("CREATE TABLE show_columns_only_identity_partition " +
                "(id int," +
                " name varchar," +
                " team varchar) WITH (partitioning = ARRAY['team'])");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM show_columns_only_identity_partition");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "integer", "", "")
                .row("name", "varchar", "", "")
                .row("team", "varchar", "partition key", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);

        // Partitioned Table with non identity partition transforms
        getQueryRunner().execute("CREATE TABLE show_columns_with_non_identity_partition " +
                "(id int," +
                " name varchar," +
                " team varchar) WITH (partitioning = ARRAY['truncate(team, 1)', 'team'])");

        actual = computeActual("SHOW COLUMNS FROM show_columns_with_non_identity_partition");

        expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "integer", "", "")
                .row("name", "varchar", "", "")
                .row("team", "varchar", "partition by truncate[1], identity", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @DataProvider(name = "timezones")
    public Object[][] timezones()
    {
        return new Object[][] {
                {"UTC", true},
                {"America/Los_Angeles", true},
                {"Asia/Shanghai", true},
                {"None", false}};
    }

    @Test(dataProvider = "timezones")
    public void testPartitionedByTimestampType(String zoneId, boolean legacyTimestamp)
    {
        Session sessionForTimeZone = sessionForTimezone(zoneId, legacyTimestamp);
        testWithAllFileFormats(sessionForTimeZone, (session, fileFormat) -> testPartitionedByTimestampTypeForFormat(session, fileFormat));
    }

    private void testPartitionedByTimestampTypeForFormat(Session session, FileFormat fileFormat)
    {
        try {
            // create iceberg table partitioned by column of TimestampType, and insert some data
            assertQuerySucceeds(session, format("create table test_partition_columns(a bigint, b timestamp) with (partitioning = ARRAY['b'], format = '%s')", fileFormat.name()));
            assertQuerySucceeds(session, "insert into test_partition_columns values(1, timestamp '1984-12-08 00:10:00'), (2, timestamp '2001-01-08 12:01:01')");

            // validate return data of TimestampType
            List<Object> timestampColumnDatas = getQueryRunner().execute(session, "select b from test_partition_columns order by a asc").getOnlyColumn().collect(Collectors.toList());
            assertEquals(timestampColumnDatas.size(), 2);
            assertEquals(timestampColumnDatas.get(0), LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            assertEquals(timestampColumnDatas.get(1), LocalDateTime.parse("2001-01-08 12:01:01", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

            // validate column of TimestampType exists in query filter
            assertEquals(getQueryRunner().execute(session, "select b from test_partition_columns where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(),
                    LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            assertEquals(getQueryRunner().execute(session, "select b from test_partition_columns where b = timestamp '2001-01-08 12:01:01'").getOnlyValue(),
                    LocalDateTime.parse("2001-01-08 12:01:01", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

            // validate column of TimestampType in system table "partitions"
            assertEquals(getQueryRunner().execute(session, "select count(*) FROM \"test_partition_columns$partitions\"").getOnlyValue(), 2L);
            assertEquals(getQueryRunner().execute(session, "select row_count from \"test_partition_columns$partitions\" where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(), 1L);
            assertEquals(getQueryRunner().execute(session, "select row_count from \"test_partition_columns$partitions\" where b = timestamp '2001-01-08 12:01:01'").getOnlyValue(), 1L);

            // validate column of TimestampType exists in delete filter
            assertUpdate(session, "delete from test_partition_columns WHERE b = timestamp '2001-01-08 12:01:01'", 1);
            timestampColumnDatas = getQueryRunner().execute(session, "select b from test_partition_columns order by a asc").getOnlyColumn().collect(Collectors.toList());
            assertEquals(timestampColumnDatas.size(), 1);
            assertEquals(timestampColumnDatas.get(0), LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            assertEquals(getQueryRunner().execute(session, "select b FROM test_partition_columns where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(),
                    LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            assertEquals(getQueryRunner().execute(session, "select count(*) from \"test_partition_columns$partitions\"").getOnlyValue(), 1L);
            assertEquals(getQueryRunner().execute(session, "select row_count from \"test_partition_columns$partitions\" where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(), 1L);
        }
        finally {
            assertQuerySucceeds(session, "drop table test_partition_columns");
        }
    }

    @Test
    public void testPartitionedByTimeType()
    {
        // create iceberg table partitioned by column of TimestampType, and insert some data
        assertQuerySucceeds("drop table if exists test_partition_columns_time");
        assertQuerySucceeds("create table test_partition_columns_time(a bigint, b time) with (partitioning = ARRAY['b'])");
        assertQuerySucceeds("insert into test_partition_columns_time values(1, time '00:10:00'), (2, time '12:01:01')");

        // validate return data of TimestampType
        List<Object> timestampColumnDatas = getQueryRunner().execute("select b from test_partition_columns_time order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(timestampColumnDatas.size(), 2);
        assertEquals(timestampColumnDatas.get(0), LocalTime.parse("00:10:00", DateTimeFormatter.ofPattern("HH:mm:ss")));
        assertEquals(timestampColumnDatas.get(1), LocalTime.parse("12:01:01", DateTimeFormatter.ofPattern("HH:mm:ss")));

        // validate column of TimestampType exists in query filter
        assertEquals(getQueryRunner().execute("select b from test_partition_columns_time where b = time '00:10:00'").getOnlyValue(),
                LocalTime.parse("00:10:00", DateTimeFormatter.ofPattern("HH:mm:ss")));
        assertEquals(getQueryRunner().execute("select b from test_partition_columns_time where b = time '12:01:01'").getOnlyValue(),
                LocalTime.parse("12:01:01", DateTimeFormatter.ofPattern("HH:mm:ss")));

        // validate column of TimestampType in system table "partitions"
        assertEquals(getQueryRunner().execute("select count(*) FROM \"test_partition_columns_time$partitions\"").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_time$partitions\" where b = time '00:10:00'").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_time$partitions\" where b = time '12:01:01'").getOnlyValue(), 1L);

        // validate column of TimestampType exists in delete filter
        assertUpdate("delete from test_partition_columns_time WHERE b = time '12:01:01'", 1);
        timestampColumnDatas = getQueryRunner().execute("select b from test_partition_columns_time order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(timestampColumnDatas.size(), 1);
        assertEquals(timestampColumnDatas.get(0), LocalTime.parse("00:10:00", DateTimeFormatter.ofPattern("HH:mm:ss")));
        assertEquals(getQueryRunner().execute("select b FROM test_partition_columns_time where b = time '00:10:00'").getOnlyValue(),
                LocalTime.parse("00:10:00", DateTimeFormatter.ofPattern("HH:mm:ss")));
        assertEquals(getQueryRunner().execute("select count(*) from \"test_partition_columns_time$partitions\"").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_time$partitions\" where b = time '00:10:00'").getOnlyValue(), 1L);

        assertQuerySucceeds("drop table test_partition_columns_time");
    }

    @Test
    public void testPartitionedByVarbinaryType()
    {
        // create iceberg table partitioned by column of VarbinaryType, and insert some data
        assertQuerySucceeds("drop table if exists test_partition_columns_varbinary");
        assertQuerySucceeds("create table test_partition_columns_varbinary(a bigint, b varbinary) with (partitioning = ARRAY['b'])");
        assertQuerySucceeds("insert into test_partition_columns_varbinary values(1, X'bcd1'), (2, X'e3bcd1')");

        // validate return data of VarbinaryType
        List<Object> varbinaryColumnDatas = getQueryRunner().execute("select b from test_partition_columns_varbinary order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(varbinaryColumnDatas.size(), 2);
        assertEquals(varbinaryColumnDatas.get(0), new byte[] {(byte) 0xbc, (byte) 0xd1});
        assertEquals(varbinaryColumnDatas.get(1), new byte[] {(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});

        // validate column of VarbinaryType exists in query filter
        assertEquals(getQueryRunner().execute("select b from test_partition_columns_varbinary where b = X'bcd1'").getOnlyValue(),
                new byte[] {(byte) 0xbc, (byte) 0xd1});
        assertEquals(getQueryRunner().execute("select b from test_partition_columns_varbinary where b = X'e3bcd1'").getOnlyValue(),
                new byte[] {(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});

        // validate column of VarbinaryType in system table "partitions"
        assertEquals(getQueryRunner().execute("select count(*) FROM \"test_partition_columns_varbinary$partitions\"").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_varbinary$partitions\" where b = X'bcd1'").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_varbinary$partitions\" where b = X'e3bcd1'").getOnlyValue(), 1L);

        // validate column of VarbinaryType exists in delete filter
        assertUpdate("delete from test_partition_columns_varbinary WHERE b = X'bcd1'", 1);
        varbinaryColumnDatas = getQueryRunner().execute("select b from test_partition_columns_varbinary order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(varbinaryColumnDatas.size(), 1);
        assertEquals(varbinaryColumnDatas.get(0), new byte[] {(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});
        assertEquals(getQueryRunner().execute("select b FROM test_partition_columns_varbinary where b = X'e3bcd1'").getOnlyValue(),
                new byte[] {(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});
        assertEquals(getQueryRunner().execute("select count(*) from \"test_partition_columns_varbinary$partitions\"").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_varbinary$partitions\" where b = X'e3bcd1'").getOnlyValue(), 1L);

        assertQuerySucceeds("drop table test_partition_columns_varbinary");
    }

    @DataProvider(name = "columnCount")
    public Object[][] getColumnCount()
    {
        return new Object[][] {{2}, {16}, {100}};
    }

    @Test(dataProvider = "columnCount")
    public void testReadWriteStatsWithColumnLimits(int columnCount)
    {
        try {
            String columns = Joiner.on(", ")
                    .join(IntStream.iterate(2, i -> i + 1).limit(columnCount - 2)
                            .mapToObj(idx -> "column_" + idx + " int")
                            .iterator());
            String comma = Strings.isNullOrEmpty(columns.trim()) ? "" : ", ";

            // The columns number of `test_stats_with_column_limits` for which metrics are collected is set to `columnCount`
            assertUpdate("CREATE TABLE test_stats_with_column_limits (column_0 int, column_1 varchar, " + columns + comma + "column_10001 varchar) with(metrics_max_inferred_column = " + columnCount + ")");
            assertTrue(getQueryRunner().tableExists(getSession(), "test_stats_with_column_limits"));
            List<String> columnNames = IntStream.iterate(0, i -> i + 1).limit(columnCount)
                    .mapToObj(idx -> "column_" + idx).collect(Collectors.toList());
            columnNames.add("column_10001");
            assertTableColumnNames("test_stats_with_column_limits", columnNames.toArray(new String[0]));

            // test that stats don't exist before analyze
            Function<Map<ColumnHandle, ColumnStatistics>, Map<String, ColumnStatistics>> remapper = (input) -> input.entrySet().stream().collect(Collectors.toMap(e -> ((IcebergColumnHandle) e.getKey()).getName(), Map.Entry::getValue));
            Map<String, ColumnStatistics> columnStats;
            TableStatistics stats = getTableStats("test_stats_with_column_limits");
            columnStats = remapper.apply(stats.getColumnStatistics());
            assertTrue(columnStats.isEmpty());

            String values1 = Joiner.on(", ")
                    .join(IntStream.iterate(2, i -> i + 1).limit(columnCount - 2)
                            .mapToObj(idx -> "100" + idx)
                            .iterator());
            String values2 = Joiner.on(", ")
                    .join(IntStream.iterate(2, i -> i + 1).limit(columnCount - 2)
                            .mapToObj(idx -> "200" + idx)
                            .iterator());
            String values3 = Joiner.on(", ")
                    .join(IntStream.iterate(2, i -> i + 1).limit(columnCount - 2)
                            .mapToObj(idx -> "300" + idx)
                            .iterator());
            // test after simple insert we get a good estimate
            assertUpdate("INSERT INTO test_stats_with_column_limits VALUES " +
                    "(1, '1001', " + values1 + comma + "'abc'), " +
                    "(2, '2001', " + values2 + comma + "'xyz'), " +
                    "(3, '3001', " + values3 + comma + "'lmnopqrst')", 3);
            getQueryRunner().execute("ANALYZE test_stats_with_column_limits");
            stats = getTableStats("test_stats_with_column_limits");
            columnStats = remapper.apply(stats.getColumnStatistics());

            // `column_0` has columns statistics
            ColumnStatistics columnStat = columnStats.get("column_0");
            assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
            assertEquals(columnStat.getNullsFraction(), Estimate.of(0.0));
            assertEquals(columnStat.getDataSize(), Estimate.unknown());

            // `column_1` has columns statistics
            columnStat = columnStats.get("column_1");
            assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
            assertEquals(columnStat.getNullsFraction(), Estimate.of(0.0));
            assertEquals(columnStat.getDataSize(), Estimate.of(12.0));

            // `column_10001` do not have column statistics as its column index is
            //  larger than the max number for which metrics are collected
            columnStat = columnStats.get("column_10001");
            assertEquals(columnStat.getDistinctValuesCount(), Estimate.unknown());
            assertEquals(columnStat.getNullsFraction(), Estimate.unknown());
            assertEquals(columnStat.getDataSize(), Estimate.unknown());
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_stats_with_column_limits");
        }
    }

    @Test
    public void testReadWriteStats()
    {
        assertUpdate("CREATE TABLE test_stats (col0 int, col_1 varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_stats"));
        assertTableColumnNames("test_stats", "col0", "col_1");

        // test that stats don't exist before analyze
        Function<Map<ColumnHandle, ColumnStatistics>, Map<String, ColumnStatistics>> remapper = (input) -> input.entrySet().stream().collect(Collectors.toMap(e -> ((IcebergColumnHandle) e.getKey()).getName(), Map.Entry::getValue));
        Map<String, ColumnStatistics> columnStats;
        TableStatistics stats = getTableStats("test_stats");
        columnStats = remapper.apply(stats.getColumnStatistics());
        assertTrue(columnStats.isEmpty());

        // test after simple insert we get a good estimate
        assertUpdate("INSERT INTO test_stats VALUES (1, 'abc'), (2, 'xyz'), (3, 'lmnopqrst')", 3);
        getQueryRunner().execute("ANALYZE test_stats");
        stats = getTableStats("test_stats");
        columnStats = remapper.apply(stats.getColumnStatistics());
        ColumnStatistics columnStat = columnStats.get("col0");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        assertEquals(columnStat.getDataSize(), Estimate.unknown());
        columnStat = columnStats.get("col_1");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        double dataSize = (double) (long) getQueryRunner().execute("SELECT sum_data_size_for_stats(col_1) FROM test_stats").getOnlyValue();
        assertEquals(columnStat.getDataSize().getValue(), dataSize);

        // test after inserting the same values, we still get the same estimate
        assertUpdate("INSERT INTO test_stats VALUES (1, 'abc'), (2, 'xyz'), (3, 'lmnopqrst')", 3);
        stats = getTableStats("test_stats");
        columnStats = remapper.apply(stats.getColumnStatistics());
        columnStat = columnStats.get("col0");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        assertEquals(columnStat.getDataSize(), Estimate.unknown());
        columnStat = columnStats.get("col_1");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        assertEquals(columnStat.getDataSize().getValue(), dataSize);

        // test after ANALYZING with the new inserts that the NDV estimate is the same and the data size matches
        getQueryRunner().execute("ANALYZE test_stats");
        stats = getTableStats("test_stats");
        columnStats = remapper.apply(stats.getColumnStatistics());
        columnStat = columnStats.get("col0");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        assertEquals(columnStat.getDataSize(), Estimate.unknown());
        columnStat = columnStats.get("col_1");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        dataSize = (double) (long) getQueryRunner().execute("SELECT sum_data_size_for_stats(col_1) FROM test_stats").getOnlyValue();
        assertEquals(columnStat.getDataSize().getValue(), dataSize);

        // test after inserting a new value, but not analyzing, the estimate is the same.
        assertUpdate("INSERT INTO test_stats VALUES (4, 'def')", 1);
        stats = getTableStats("test_stats");
        columnStats = remapper.apply(stats.getColumnStatistics());
        columnStat = columnStats.get("col0");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        assertEquals(columnStat.getDataSize(), Estimate.unknown());
        columnStat = columnStats.get("col_1");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(3.0));
        assertEquals(columnStat.getDataSize().getValue(), dataSize);

        // test that after analyzing, the updates stats show up.
        getQueryRunner().execute("ANALYZE test_stats");
        stats = getTableStats("test_stats");
        columnStats = remapper.apply(stats.getColumnStatistics());
        columnStat = columnStats.get("col0");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(4.0));
        assertEquals(columnStat.getDataSize(), Estimate.unknown());
        columnStat = columnStats.get("col_1");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(4.0));
        dataSize = (double) (long) getQueryRunner().execute("SELECT sum_data_size_for_stats(col_1) FROM test_stats").getOnlyValue();
        assertEquals(columnStat.getDataSize().getValue(), dataSize);

        // test adding a null value is successful, and analyze still runs successfully
        assertUpdate("INSERT INTO test_stats VALUES (NULL, NULL)", 1);
        assertQuerySucceeds("ANALYZE test_stats");
        stats = getTableStats("test_stats");
        columnStats = remapper.apply(stats.getColumnStatistics());
        columnStat = columnStats.get("col0");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(4.0));
        assertEquals(columnStat.getDataSize(), Estimate.unknown());
        columnStat = columnStats.get("col_1");
        assertEquals(columnStat.getDistinctValuesCount(), Estimate.of(4.0));
        dataSize = (double) (long) getQueryRunner().execute("SELECT sum_data_size_for_stats(col_1) FROM test_stats").getOnlyValue();
        assertEquals(columnStat.getDataSize().getValue(), dataSize);

        assertUpdate("DROP TABLE test_stats");
    }

    @Test
    public void testReadWriteNDVsComplexTypes()
    {
        assertUpdate("CREATE TABLE test_stat_ndv_complex (col0 int, col1 date, col2 varchar, col3 row(c0 int))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_stat_ndv_complex"));
        assertTableColumnNames("test_stat_ndv_complex", "col0", "col1", "col2", "col3");

        // test that stats don't exist before analyze
        TableStatistics stats = getTableStats("test_stat_ndv_complex");
        assertTrue(stats.getColumnStatistics().isEmpty());

        // test after simple insert we get a good estimate
        assertUpdate("INSERT INTO test_stat_ndv_complex VALUES (0, current_date, 't1', row(0))", 1);
        getQueryRunner().execute("ANALYZE test_stat_ndv_complex");
        stats = getTableStats("test_stat_ndv_complex");
        assertEquals(columnStatsFor(stats, "col0").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col1").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col2").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col3").getDistinctValuesCount(), Estimate.unknown());

        // test after inserting the same values, we still get the same estimate
        assertUpdate("INSERT INTO test_stat_ndv_complex VALUES (0, current_date, 't1', row(0))", 1);
        stats = getTableStats("test_stat_ndv_complex");
        assertEquals(columnStatsFor(stats, "col0").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col1").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col2").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col3").getDistinctValuesCount(), Estimate.unknown());

        // test after ANALYZING with the new inserts that the NDV estimate is the same
        getQueryRunner().execute("ANALYZE test_stat_ndv_complex");
        stats = getTableStats("test_stat_ndv_complex");
        assertEquals(columnStatsFor(stats, "col0").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col1").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col2").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col3").getDistinctValuesCount(), Estimate.unknown());

        // test after inserting a new value, but not analyzing, the estimate is the same.
        assertUpdate("INSERT INTO test_stat_ndv_complex VALUES (1, current_date + interval '1' day, 't2', row(1))", 1);
        stats = getTableStats("test_stat_ndv_complex");
        assertEquals(columnStatsFor(stats, "col0").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col1").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col2").getDistinctValuesCount(), Estimate.of(1.0));
        assertEquals(columnStatsFor(stats, "col3").getDistinctValuesCount(), Estimate.unknown());

        // test that after analyzing, the updates stats show up.
        getQueryRunner().execute("ANALYZE test_stat_ndv_complex");
        stats = getTableStats("test_stat_ndv_complex");
        assertEquals(columnStatsFor(stats, "col0").getDistinctValuesCount(), Estimate.of(2.0));
        assertEquals(columnStatsFor(stats, "col1").getDistinctValuesCount(), Estimate.of(2.0));
        assertEquals(columnStatsFor(stats, "col2").getDistinctValuesCount(), Estimate.of(2.0));
        assertEquals(columnStatsFor(stats, "col3").getDistinctValuesCount(), Estimate.unknown());

        // test adding a null value is successful, and analyze still runs successfully
        assertUpdate("INSERT INTO test_stat_ndv_complex VALUES (NULL, NULL, NULL, NULL)", 1);
        assertQuerySucceeds("ANALYZE test_stat_ndv_complex");
        stats = getTableStats("test_stat_ndv_complex");
        assertEquals(columnStatsFor(stats, "col0").getDistinctValuesCount(), Estimate.of(2.0));
        assertEquals(columnStatsFor(stats, "col1").getDistinctValuesCount(), Estimate.of(2.0));
        assertEquals(columnStatsFor(stats, "col2").getDistinctValuesCount(), Estimate.of(2.0));
        assertEquals(columnStatsFor(stats, "col3").getDistinctValuesCount(), Estimate.unknown());

        assertUpdate("DROP TABLE test_stat_ndv_complex");
    }

    @Test
    public void testNDVsAtSnapshot()
    {
        assertUpdate("CREATE TABLE test_stat_snap (col0 int, col1 varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_stat_snap"));
        assertTableColumnNames("test_stat_snap", "col0", "col1");
        assertEquals(getTableSnapshots("test_stat_snap").size(), 0);

        assertQuerySucceeds("ANALYZE test_stat_snap");
        assertUpdate("INSERT INTO test_stat_snap VALUES (0, '0')", 1);
        assertQuerySucceeds("ANALYZE test_stat_snap");
        assertUpdate("INSERT INTO test_stat_snap VALUES (1, '1')", 1);
        assertQuerySucceeds("ANALYZE test_stat_snap");
        assertUpdate("INSERT INTO test_stat_snap VALUES (2, '2')", 1);
        assertQuerySucceeds("ANALYZE test_stat_snap");
        assertUpdate("INSERT INTO test_stat_snap VALUES (3, '3')", 1);
        assertQuerySucceeds("ANALYZE test_stat_snap");
        assertEquals(getTableSnapshots("test_stat_snap").size(), 4);

        List<Long> snaps = getTableSnapshots("test_stat_snap");
        for (int i = 0; i < snaps.size(); i++) {
            TableStatistics statistics = getTableStats("test_stat_snap", Optional.of(snaps.get(i)));
            // assert either case as we don't have good control over the timing of when statistics files are written
            ColumnStatistics col0Stats = columnStatsFor(statistics, "col0");
            ColumnStatistics col1Stats = columnStatsFor(statistics, "col1");
            final int idx = i;
            assertEither(
                    () -> assertEquals(col0Stats.getDistinctValuesCount(), Estimate.of(idx)),
                    () -> assertEquals(col0Stats.getDistinctValuesCount(), Estimate.of(idx + 1)));
            assertEither(
                    () -> assertEquals(col1Stats.getDistinctValuesCount(), Estimate.of(idx)),
                    () -> assertEquals(col1Stats.getDistinctValuesCount(), Estimate.of(idx + 1)));
        }
        assertUpdate("DROP TABLE test_stat_snap");
    }

    @Test
    public void testStatsByDistance()
    {
        assertUpdate("CREATE TABLE test_stat_dist (col0 int)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_stat_dist"));
        assertTableColumnNames("test_stat_dist", "col0");
        assertEquals(getTableSnapshots("test_stat_dist").size(), 0);

        assertUpdate("INSERT INTO test_stat_dist VALUES 0", 1);
        assertQuerySucceeds("ANALYZE test_stat_dist");
        assertUpdate("INSERT INTO test_stat_dist VALUES 1", 1);
        assertUpdate("INSERT INTO test_stat_dist VALUES 2", 1);
        assertUpdate("INSERT INTO test_stat_dist VALUES 3", 1);
        assertUpdate("INSERT INTO test_stat_dist VALUES 4", 1);
        assertUpdate("INSERT INTO test_stat_dist VALUES 5", 1);
        assertQuerySucceeds("ANALYZE test_stat_dist");
        assertEquals(getTableSnapshots("test_stat_dist").size(), 6);
        List<Long> snapshots = getTableSnapshots("test_stat_dist");
        // set a high weight so the weighting calculation is mostly done by record count
        Session weightedSession = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT, "10000000")
                .build();
        Function<Integer, Estimate> ndvs = (x) -> columnStatsFor(getTableStats("test_stat_dist", Optional.of(snapshots.get(x)), weightedSession, Optional.empty()), "col0")
                .getDistinctValuesCount();
        assertEquals(ndvs.apply(0).getValue(), 1);
        assertEquals(ndvs.apply(1).getValue(), 1);
        assertEquals(ndvs.apply(2).getValue(), 1);
        assertEquals(ndvs.apply(3).getValue(), 6);
        assertEquals(ndvs.apply(4).getValue(), 6);
        assertEquals(ndvs.apply(5).getValue(), 6);
        assertUpdate("DROP TABLE test_stat_dist");
    }

    @Test
    public void testStatsDataSizePrimitives()
    {
        assertUpdate("CREATE TABLE test_stat_data_size (c0 int, c1 bigint, c2 double, c3 decimal(4, 0), c4 varchar, c5 varchar(10), c6 date, c7 time, c8 timestamp, c10 boolean)");
        assertUpdate("INSERT INTO test_stat_data_size VALUES (0, 1, 2.0, CAST(4.01 as decimal(4, 0)), 'testvc', 'testvc10', date '2024-03-14', localtime, localtimestamp, TRUE)", 1);
        assertQuerySucceeds("ANALYZE test_stat_data_size");
        TableStatistics stats = getTableStats("test_stat_data_size");
        stats.getColumnStatistics().entrySet().stream()
                .filter((e) -> ((IcebergColumnHandle) e.getKey()).getColumnType() != SYNTHESIZED)
                .forEach((entry) -> {
                    IcebergColumnHandle handle = (IcebergColumnHandle) entry.getKey();
                    ColumnStatistics stat = entry.getValue();
                    if (handle.getType() instanceof FixedWidthType) {
                        assertEquals(stat.getDataSize(), Estimate.unknown());
                    }
                    else {
                        assertNotEquals(stat.getDataSize(), Estimate.unknown(), String.format("for column %s", handle));
                        assertTrue(stat.getDataSize().getValue() > 0);
                    }
                });

        getQueryRunner().execute("DROP TABLE test_stat_data_size");
    }

    private static void assertEither(Runnable first, Runnable second)
    {
        try {
            first.run();
        }
        catch (AssertionError e) {
            second.run();
        }
    }

    private List<Long> getTableSnapshots(String tableName)
    {
        MaterializedResult result = getQueryRunner().execute(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at", tableName));
        return result.getOnlyColumn().map(Long.class::cast).collect(Collectors.toList());
    }

    private TableStatistics getTableStats(String name)
    {
        return getTableStats(name, Optional.empty());
    }

    private TableStatistics getTableStats(String name, Optional<Long> snapshot)
    {
        return getTableStats(name, snapshot, getSession(), Optional.empty());
    }

    private TableStatistics getTableStats(String name, Optional<Long> snapshot, Session session, Optional<List<String>> columns)
    {
        TransactionId transactionId = getQueryRunner().getTransactionManager().beginTransaction(false);
        Session metadataSession = session.beginTransactionId(
                transactionId,
                getQueryRunner().getTransactionManager(),
                new AllowAllAccessControl());
        Metadata metadata = getDistributedQueryRunner().getMetadata();
        MetadataResolver resolver = metadata.getMetadataResolver(metadataSession);
        String tableName = snapshot.map(snap -> format("%s@%d", name, snap)).orElse(name);
        String qualifiedName = format("%s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        TableHandle handle = resolver.getTableHandle(QualifiedObjectName.valueOf(qualifiedName)).get();
        return metadata.getTableStatistics(metadataSession,
                handle,
                new ArrayList<>(columns
                        .map(columnSet -> Maps.filterKeys(resolver.getColumnHandles(handle), columnSet::contains))
                        .orElse(resolver.getColumnHandles(handle)).values()),
                Constraint.alwaysTrue());
    }

    private static ColumnStatistics columnStatsFor(TableStatistics statistics, String name)
    {
        return statistics.getColumnStatistics().entrySet()
                .stream().filter(entry -> ((IcebergColumnHandle) entry.getKey()).getName().equals(name))
                .map(Map.Entry::getValue)
                .findFirst()
                .get();
    }

    @DataProvider(name = "fileFormat")
    public Object[][] getFileFormat()
    {
        return new Object[][] {{"PARQUET"}, {"ORC"}};
    }

    @Test(dataProvider = "fileFormat")
    public void testTableWithPositionDelete(String fileFormat)
            throws Exception
    {
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);
        String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 0);
        testCheckDeleteFiles(icebergTable, 1, ImmutableList.of(POSITION_DELETES));
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 24");
        assertQuery("SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE nationkey != 0");

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 8);
        testCheckDeleteFiles(icebergTable, 2, ImmutableList.of(POSITION_DELETES, POSITION_DELETES));
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 23");
        assertQuery("SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE nationkey not in (0 ,8)");
    }

    @DataProvider(name = "equalityDeleteOptions")
    public Object[][] equalityDeleteDataProvider()
    {
        return new Object[][] {
                {"PARQUET", false},
                {"PARQUET", true},
                {"ORC", false},
                {"ORC", true}};
    }

    private Session deleteAsJoinEnabled(boolean joinRewriteEnabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, DELETE_AS_JOIN_REWRITE_ENABLED, String.valueOf(joinRewriteEnabled))
                .build();
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeleteWithPartitionColumnMissingInSelect(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (format = '" + fileFormat + "', partitioning=ARRAY['a'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002'), (2, '1010'), (3, '1003')", 4);

            Table icebergTable = updateTable(tableName);
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 2, "b", "1002"), ImmutableMap.of("a", 2));
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("b", "1010"), ImmutableMap.of("a", 2));
            assertQuery(session, "SELECT b FROM " + tableName, "VALUES ('1001'), ('1003')");
            assertQuery(session, "SELECT b FROM " + tableName + " WHERE a > 1", "VALUES ('1003')");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c int WITH (partitioning = 'identity')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, '1004', 1), (6, '1006', 2), (6, '1009', 2)", 3);
            icebergTable = updateTable(tableName);
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 2, "b", "1006"),
                    ImmutableMap.of("a", 6, "c", 2));
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("b", "1009"),
                    ImmutableMap.of("a", 6, "c", 2));
            assertQuery(session, "SELECT a, b FROM " + tableName, "VALUES (1, '1001'), (3, '1003'), (6, '1004')");
            assertQuery(session, "SELECT a, b FROM " + tableName + " WHERE a in (1, 6) and c < 3", "VALUES (6, '1004')");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN d varchar WITH (partitioning = 'truncate(2)')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, '1004', 1, 'th001'), (6, '1006', 2, 'th002'), (6, '1006', 3, 'ti003')", 3);
            icebergTable = updateTable(tableName);
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 1, "d", "th001"),
                    ImmutableMap.of("a", 6, "c", 1, "d_trunc", "th"));
            testCheckDeleteFiles(icebergTable, 5, ImmutableList.of(EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES));
            assertQuery(session, "SELECT a, b, d FROM " + tableName, "VALUES (1, '1001', NULL), (3, '1003', NULL), (6, '1004', NULL), (6, '1006', 'th002'), (6, '1006', 'ti003')");
            assertQuery(session, "SELECT a, b, d FROM " + tableName + " WHERE a in (1, 6) and d > 'th000'", "VALUES (6, '1006', 'th002'), (6, '1006', 'ti003')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testTableWithEqualityDelete(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_equality_delete" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        testCheckDeleteFiles(icebergTable, 1, ImmutableList.of(EQUALITY_DELETES));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey != 1");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testTableWithEqualityDeleteDifferentColumnOrder(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        // Specify equality delete filter with different column order from table definition
        String tableName = "test_v2_equality_delete_different_order" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L, "name", "ARGENTINA"));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE name != 'ARGENTINA'");
        // natiokey is before the equality delete column in the table schema, comment is after
        assertQuery(session, "SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE name != 'ARGENTINA'");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testTableWithEqualityDeleteAndGroupByAndLimit(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        Session disable = deleteAsJoinEnabled(false);
        // Specify equality delete filter with different column order from table definition
        String tableName = "test_v2_equality_delete_different_order" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L, "name", "ARGENTINA"));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE name != 'ARGENTINA'");

        // Test group by
        assertQuery(session, "SELECT nationkey FROM " + tableName + " group by nationkey", "VALUES(0),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24)");
        // Test group by with limit
        assertQueryWithSameQueryRunner(session, "SELECT nationkey FROM " + tableName + " group by nationkey limit 100", disable);
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testTableWithPositionDeleteAndEqualityDelete(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);
        String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 0);
        testCheckDeleteFiles(icebergTable, 1, ImmutableList.of(POSITION_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 24");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE nationkey != 0");

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        testCheckDeleteFiles(icebergTable, 2, ImmutableList.of(POSITION_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1 AND nationkey != 0");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey != 1 AND nationkey != 0");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testPartitionedTableWithEqualityDelete(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_equality_delete" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['nationkey'], format = '" + fileFormat + "') " + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L, "nationkey", 1L), ImmutableMap.of("nationkey", 1L));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1 or nationkey != 1 ");
        assertQuery(session, "SELECT nationkey, comment FROM " + tableName, "SELECT nationkey, comment FROM nation WHERE regionkey != 1 or nationkey != 1");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeletesWithPartitions(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "', partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        List<Long> partitions = Arrays.asList(1L, 2L, 3L, 17L, 24L);
        for (long nationKey : partitions) {
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L), ImmutableMap.of("nationkey", nationKey));
        }
        testCheckDeleteFiles(icebergTable, partitions.size(), partitions.stream().map(i -> EQUALITY_DELETES).collect(Collectors.toList()));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey != 1");
        assertQuery(session, "SELECT name FROM " + tableName, "SELECT name FROM nation WHERE regionkey != 1");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeletesWithHiddenPartitions(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "', partitioning = ARRAY['bucket(nationkey,100)']) AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        PartitionTransforms.ColumnTransform columnTransform = PartitionTransforms.getColumnTransform(icebergTable.spec().fields().get(0), BIGINT);
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(5);
        List<Long> partitions = Arrays.asList(1L, 2L, 3L, 17L, 24L);
        partitions.forEach(p -> BIGINT.writeLong(builder, p));
        Block partitionsBlock = columnTransform.getTransform().apply(builder.build());
        for (int i = 0; i < partitionsBlock.getPositionCount(); i++) {
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L), ImmutableMap.of("nationkey_bucket", partitionsBlock.getInt(i)));
            String updatedPartitions = partitions.stream().limit(i + 1).map(Object::toString).collect(Collectors.joining(",", "(", ")"));
            assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE NOT(regionkey = 1 AND nationkey IN " + updatedPartitions + ")");
        }
        testCheckDeleteFiles(icebergTable, partitions.size(), partitions.stream().map(i -> EQUALITY_DELETES).collect(Collectors.toList()));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey != 1");
        assertQuery(session, "SELECT name FROM " + tableName, "SELECT name FROM nation WHERE regionkey != 1");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeletesWithCompositeKey(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 0L, "name", "ALGERIA"));
        testCheckDeleteFiles(icebergTable, 1, Stream.generate(() -> EQUALITY_DELETES).limit(1).collect(Collectors.toList()));
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE NOT(regionkey = 0 AND name = 'ALGERIA')");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE NOT(regionkey = 0 AND name = 'ALGERIA')");
        assertQuery(session, "SELECT name FROM " + tableName, "SELECT name FROM nation WHERE NOT(regionkey = 0 AND name = 'ALGERIA')");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeletesWithMultipleDeleteSchemas(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("nationkey", 10L));
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 2L, "nationkey", 9L));
        testCheckDeleteFiles(icebergTable, 3, Stream.generate(() -> EQUALITY_DELETES).limit(3).collect(Collectors.toList()));
        assertQuery(session, "SELECT \"$data_sequence_number\", regionkey, nationkey FROM \"" + tableName + "$equality_deletes\"", "VALUES (2, 1, null), (3, null, 10), (4, 2, 9)");
        assertQuery(session, "SELECT * FROM " + tableName, "SELECT * FROM nation WHERE NOT(regionkey = 2 AND nationkey = 9) AND nationkey <> 10 AND regionkey <> 1");
        assertQuery(session, "SELECT regionkey FROM " + tableName, "SELECT regionkey FROM nation WHERE NOT(regionkey = 2 AND nationkey = 9) AND nationkey <> 10 AND regionkey <> 1");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE NOT(regionkey = 2 AND nationkey = 9) AND nationkey <> 10 AND regionkey <> 1");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testTableWithPositionDeletesAndEqualityDeletes(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + tableName + " with (format = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);
        String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 0);
        testCheckDeleteFiles(icebergTable, 1, ImmutableList.of(POSITION_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 24");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE nationkey != 0");

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        testCheckDeleteFiles(icebergTable, 2, ImmutableList.of(POSITION_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 19");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey != 1 AND nationkey != 0");

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 7);
        testCheckDeleteFiles(icebergTable, 3, ImmutableList.of(POSITION_DELETES, POSITION_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 18");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey != 1 AND nationkey NOT IN (0, 7)");

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 2L));
        testCheckDeleteFiles(icebergTable, 4, ImmutableList.of(POSITION_DELETES, POSITION_DELETES, EQUALITY_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 13");
        assertQuery(session, "SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey NOT IN (1, 2) AND nationkey NOT IN (0, 7)");
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeletesWithHiddenPartitionsEvolution(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (format = '" + fileFormat + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002'), (3, '1003')", 3);

        Table icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 2));
        assertQuery(session, "SELECT * FROM " + tableName, "VALUES (1, '1001'), (3, '1003')");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c int WITH (partitioning = 'bucket(2)')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (6, '1004', 1), (6, '1006', 2)", 2);
        icebergTable = updateTable(tableName);
        PartitionTransforms.ColumnTransform columnTransform = PartitionTransforms.getColumnTransform(icebergTable.spec().fields().get(0), INTEGER);
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(1);
        List<Integer> partitions = Arrays.asList(1, 2);
        partitions.forEach(p -> BIGINT.writeLong(builder, p));
        Block partitionsBlock = columnTransform.getTransform().apply(builder.build());
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 2),
                ImmutableMap.of("c_bucket", partitionsBlock.getInt(0)));
        assertQuery(session, "SELECT * FROM " + tableName, "VALUES (1, '1001', NULL), (3, '1003', NULL), (6, '1004', 1)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN d varchar WITH (partitioning = 'truncate(2)')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (6, '1004', 1, 'th001'), (6, '1006', 2, 'th002')", 2);
        icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 1),
                ImmutableMap.of("c_bucket", partitionsBlock.getInt(0), "d_trunc", "th"));
        testCheckDeleteFiles(icebergTable, 3, ImmutableList.of(EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT * FROM " + tableName, "VALUES (1, '1001', NULL, NULL), (3, '1003', NULL, NULL), (6, '1004', 1, NULL), (6, '1006', 2, 'th002')");
    }

    @Test
    public void testPartShowStatsWithFilters()
    {
        assertQuerySucceeds("CREATE TABLE showstatsfilters (i int) WITH (partitioning = ARRAY['i'])");
        assertQuerySucceeds("INSERT INTO showstatsfilters VALUES 1, 2, 3, 4, 5, 6, 7, 7, 7, 7");
        assertQuerySucceeds("ANALYZE showstatsfilters");

        MaterializedResult statsTable = getQueryRunner().execute("SHOW STATS for showstatsfilters");
        MaterializedRow columnStats = statsTable.getMaterializedRows().get(0);
        assertEquals(columnStats.getField(2), 7.0); // ndvs;
        assertEquals(columnStats.getField(3), 0.0); // nulls
        assertEquals(columnStats.getField(5), "1"); // min
        assertEquals(columnStats.getField(6), "7"); // max

        // EQ
        statsTable = getQueryRunner().execute("SHOW STATS for (SELECT * FROM showstatsfilters WHERE i = 7)");
        columnStats = statsTable.getMaterializedRows().get(0);
        assertEquals(columnStats.getField(5), "7"); // min
        assertEquals(columnStats.getField(6), "7"); // max
        assertEquals(columnStats.getField(3), 0.0); // nulls
        assertEquals((double) columnStats.getField(2), 7.0d * (4.0d / 10.0d), 1E-8); // ndvs;

        // LT
        statsTable = getQueryRunner().execute("SHOW STATS for (SELECT * FROM showstatsfilters WHERE i < 7)");
        columnStats = statsTable.getMaterializedRows().get(0);
        assertEquals(columnStats.getField(5), "1"); // min
        assertEquals(columnStats.getField(6), "6"); // max
        assertEquals(columnStats.getField(3), 0.0); // nulls
        assertEquals((double) columnStats.getField(2), 7.0d * (6.0d / 10.0d), 1E-8); // ndvs;

        // LTE
        statsTable = getQueryRunner().execute("SHOW STATS for (SELECT * FROM showstatsfilters WHERE i <= 7)");
        columnStats = statsTable.getMaterializedRows().get(0);
        assertEquals(columnStats.getField(5), "1"); // min
        assertEquals(columnStats.getField(6), "7"); // max
        assertEquals(columnStats.getField(3), 0.0); // nulls
        assertEquals(columnStats.getField(2), 7.0d); // ndvs;

        // GT
        statsTable = getQueryRunner().execute("SHOW STATS for (SELECT * FROM showstatsfilters WHERE i > 7)");
        columnStats = statsTable.getMaterializedRows().get(0);
        assertEquals(columnStats.getField(5), null); // min
        assertEquals(columnStats.getField(6), null); // max
        assertEquals(columnStats.getField(3), null); // nulls
        assertEquals(columnStats.getField(2), null); // ndvs;

        // GTE
        statsTable = getQueryRunner().execute("SHOW STATS for (SELECT * FROM showstatsfilters WHERE i >= 7)");
        columnStats = statsTable.getMaterializedRows().get(0);
        assertEquals(columnStats.getField(5), "7"); // min
        assertEquals(columnStats.getField(6), "7"); // max
        assertEquals(columnStats.getField(3), 0.0); // nulls
        assertEquals((double) columnStats.getField(2), 7.0d * (4.0d / 10.0d), 1E-8); // ndvs;

        assertQuerySucceeds("DROP TABLE showstatsfilters");
    }

    @Test
    public void testMetadataDeleteOnUnPartitionedTableWithDeleteFiles()
    {
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (format_version = '2', delete_mode = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002'), (3, '1003')", 3);

            // execute row level deletion
            assertUpdate("DELETE FROM " + tableName + " WHERE b in ('1002', '1003')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, '1001')");

            Table icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 1);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 1);

            // execute whole table metadata deletion
            assertUpdate("DELETE FROM " + tableName, 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");

            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 0);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testMetadataDeleteOnPartitionedTableWithDeleteFiles()
    {
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (format_version = '2', delete_mode = 'merge-on-read', partitioning = ARRAY['a'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002'), (3, '1003')", 3);

            // execute row level deletion
            assertUpdate("DELETE FROM " + tableName + " WHERE b in ('1002', '1003')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, '1001')");

            Table icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 3);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 2);

            // execute metadata deletion with filter
            assertUpdate("DELETE FROM " + tableName + " WHERE a in (2, 3)", 0);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, '1001')");

            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 1);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);

            // execute whole table metadata deletion
            assertUpdate("DELETE FROM " + tableName, 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");

            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 0);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testMetadataDeleteOnV2MorTableWithEmptyUnsupportedSpecs()
    {
        String tableName = "test_empty_partition_spec_table";
        try {
            // Create a table with no partition
            assertUpdate("CREATE TABLE " + tableName + " (a INTEGER, b VARCHAR) WITH (format_version = '2', delete_mode = 'merge-on-read')");

            // Do not insert data, and evaluate the partition spec by adding a partition column `c`
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c INTEGER WITH (partitioning = 'identity')");

            // Insert data under the new partition spec
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001', 1), (2, '1002', 2), (3, '1003', 3), (4, '1004', 4)", 4);

            Table icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 4);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);

            // Do metadata delete on partition column `c`, because the initial partition spec contains no data
            assertUpdate("DELETE FROM " + tableName + " WHERE c in (1, 3)", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (2, '1002', 2), (4, '1004', 4)");

            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 2);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testMetadataDeleteOnV2MorTableWithUnsupportedSpecsWhoseDataAllDeleted()
    {
        String tableName = "test_data_deleted_partition_spec_table";
        try {
            // Create a table with partition column `a`, and insert some data under this partition spec
            assertUpdate("CREATE TABLE " + tableName + " (a INTEGER, b VARCHAR) WITH (format_version = '2', delete_mode = 'merge-on-read', partitioning = ARRAY['a'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002')", 2);

            Table icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 2);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);

            // Evaluate the partition spec by adding a partition column `c`, and insert some data under the new partition spec
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c INTEGER WITH (partitioning = 'identity')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, '1003', 3), (4, '1004', 4), (5, '1005', 5)", 3);

            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 5);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);

            // Execute row level delete with filter on column `c`, because we have data with old partition spec
            assertUpdate("DELETE FROM " + tableName + " WHERE c > 3", 2);
            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 5);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 2);

            // Do metadata delete on column `a`, because all partition specs contains partition column `a`
            assertUpdate("DELETE FROM " + tableName + " WHERE a in (1, 2)", 2);
            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 3);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 2);

            // Then do metadata delete on column `c`, because the old partition spec contains no data now
            assertUpdate("DELETE FROM " + tableName + " WHERE c > 3", 0);
            assertQuery("SELECT * FROM " + tableName, "VALUES (3, '1003', 3)");
            icebergTable = loadTable(tableName);
            assertHasDataFiles(icebergTable.currentSnapshot(), 1);
            assertHasDeleteFiles(icebergTable.currentSnapshot(), 0);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testMetadataVersionsMaintainingProperties()
            throws Exception
    {
        String settingTableName = "test_table_with_setting_properties";
        String defaultTableName = "test_table_with_default_setting_properties";
        try {
            // Create a table with setting properties that maintain only 1 previous metadata version in current metadata,
            //  and delete unuseful metadata files after each commit
            assertUpdate("CREATE TABLE " + settingTableName + " (a INTEGER, b VARCHAR)" +
                    " WITH (metadata_previous_versions_max = 1, metadata_delete_after_commit = true)");

            // Create a table with default table properties that maintain 100 previous metadata versions in current metadata,
            //  and do not automatically delete any metadata files
            assertUpdate("CREATE TABLE " + defaultTableName + " (a INTEGER, b VARCHAR)");

            assertUpdate("INSERT INTO " + settingTableName + " VALUES (1, '1001'), (2, '1002')", 2);
            assertUpdate("INSERT INTO " + settingTableName + " VALUES (3, '1003'), (4, '1004')", 2);
            assertUpdate("INSERT INTO " + settingTableName + " VALUES (5, '1005'), (6, '1006')", 2);
            assertUpdate("INSERT INTO " + settingTableName + " VALUES (7, '1007'), (8, '1008')", 2);
            assertUpdate("INSERT INTO " + settingTableName + " VALUES (9, '1009'), (10, '1010')", 2);

            assertUpdate("INSERT INTO " + defaultTableName + " VALUES (1, '1001'), (2, '1002')", 2);
            assertUpdate("INSERT INTO " + defaultTableName + " VALUES (3, '1003'), (4, '1004')", 2);
            assertUpdate("INSERT INTO " + defaultTableName + " VALUES (5, '1005'), (6, '1006')", 2);
            assertUpdate("INSERT INTO " + defaultTableName + " VALUES (7, '1007'), (8, '1008')", 2);
            assertUpdate("INSERT INTO " + defaultTableName + " VALUES (9, '1009'), (10, '1010')", 2);

            Table settingTable = loadTable(settingTableName);
            TableMetadata settingTableMetadata = ((BaseTable) settingTable).operations().current();
            // Table `test_table_with_setting_properties`'s current metadata only record 1 previous metadata file
            assertEquals(settingTableMetadata.previousFiles().size(), 1);

            Table defaultTable = loadTable(defaultTableName);
            TableMetadata defaultTableMetadata = ((BaseTable) defaultTable).operations().current();
            // Table `test_table_with_default_setting_properties`'s current metadata record all 5 previous metadata files
            assertEquals(defaultTableMetadata.previousFiles().size(), 5);

            FileSystem fileSystem = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), new org.apache.hadoop.fs.Path(settingTable.location()));

            // Table `test_table_with_setting_properties`'s all existing metadata files count is 2
            FileStatus[] settingTableFiles = fileSystem.listStatus(new org.apache.hadoop.fs.Path(settingTable.location(), "metadata"), name -> name.getName().contains(METADATA_FILE_EXTENSION));
            assertEquals(settingTableFiles.length, 2);

            // Table `test_table_with_default_setting_properties`'s all existing metadata files count is 6
            FileStatus[] defaultTableFiles = fileSystem.listStatus(new org.apache.hadoop.fs.Path(defaultTable.location(), "metadata"), name -> name.getName().contains(METADATA_FILE_EXTENSION));
            assertEquals(defaultTableFiles.length, 6);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + settingTableName);
        }
    }

    @DataProvider(name = "decimalVectorReader")
    public Object[] decimalVectorReader()
    {
        return new Object[] {true, false};
    }

    private Session decimalVectorReaderEnabledSession(boolean decimalVectorReaderEnabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, String.valueOf(decimalVectorReaderEnabled))
                .build();
    }

    @Test(dataProvider = "decimalVectorReader")
    public void testDecimal(boolean decimalVectorReaderEnabled)
    {
        String tableName = "test_decimal_vector_reader";
        try {
            // Create a table with decimal column
            assertUpdate("CREATE TABLE " + tableName + " (short_decimal_column_int32 decimal(5,2), short_decimal_column_int64 decimal(16, 4), long_decimal_column decimal(19, 5))");

            String values = " VALUES (cast(-1.00 as decimal(5,2)), null, cast(9999999999.123 as decimal(19, 5)))," +
                    "(cast(1.00 as decimal(5,2)), cast(121321 as decimal(16, 4)), null)," +
                    "(cast(-1.00 as decimal(5,2)), cast(-1215789.45 as decimal(16, 4)), cast(1234584.21 as decimal(19, 5)))," +
                    "(cast(1.00 as decimal(5,2)), cast(-67867878.12 as decimal(16, 4)), cast(-9999999999.123 as decimal(19, 5)))";

            // Insert data to table
            assertUpdate("INSERT INTO " + tableName + values, 4);

            Session session = decimalVectorReaderEnabledSession(decimalVectorReaderEnabled);
            assertQuery(session, "SELECT * FROM " + tableName, values);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testRefsTable()
    {
        assertUpdate("CREATE TABLE test_table_references (id1 BIGINT, id2 BIGINT)");
        assertUpdate("INSERT INTO test_table_references VALUES (0, 00), (1, 10), (2, 20)", 3);

        Table icebergTable = loadTable("test_table_references");
        icebergTable.manageSnapshots().createBranch("testBranch").commit();

        assertUpdate("INSERT INTO test_table_references VALUES (3, 30), (4, 40), (5, 50)", 3);

        assertEquals(icebergTable.refs().size(), 2);
        icebergTable.manageSnapshots().createTag("testTag", icebergTable.currentSnapshot().snapshotId()).commit();

        assertEquals(icebergTable.refs().size(), 3);
        assertUpdate("INSERT INTO test_table_references VALUES (6, 60), (7, 70), (8, 80)", 3);
        assertQuery("SELECT count(*) FROM \"test_table_references$refs\"", "VALUES 3");

        assertQuery("SELECT count(*) FROM test_table_references FOR SYSTEM_VERSION AS OF 'testBranch'", "VALUES 3");
        assertQuery("SELECT count(*) FROM test_table_references FOR SYSTEM_VERSION AS OF 'testTag'", "VALUES 6");
        assertQuery("SELECT count(*) FROM test_table_references FOR SYSTEM_VERSION AS OF 'main'", "VALUES 9");

        assertQuery("SELECT * from \"test_table_references$refs\" where name = 'testBranch' and type = 'BRANCH'",
                format("VALUES('%s', '%s', %s, %s, %s, %s)",
                        "testBranch",
                        "BRANCH",
                        icebergTable.refs().get("testBranch").snapshotId(),
                        icebergTable.refs().get("testBranch").maxRefAgeMs(),
                        icebergTable.refs().get("testBranch").minSnapshotsToKeep(),
                        icebergTable.refs().get("testBranch").maxSnapshotAgeMs()));

        assertQuery("SELECT * from \"test_table_references$refs\" where type = 'TAG'",
                format("VALUES('%s', '%s', %s, %s, %s, %s)",
                        "testTag",
                        "TAG",
                        icebergTable.refs().get("testTag").snapshotId(),
                        icebergTable.refs().get("testTag").maxRefAgeMs(),
                        icebergTable.refs().get("testTag").minSnapshotsToKeep(),
                        icebergTable.refs().get("testTag").maxSnapshotAgeMs()));

        // test branch & tag access when schema is changed
        assertUpdate("ALTER TABLE test_table_references DROP COLUMN id2");
        assertUpdate("ALTER TABLE test_table_references ADD COLUMN id2_new BIGINT");

        // since current table schema is changed from col id2 to id2_new
        assertQuery("SELECT * FROM test_table_references where id1=1", "VALUES(1, NULL)");
        assertQuery("SELECT * FROM test_table_references FOR SYSTEM_VERSION AS OF 'testBranch' where id1=1", "VALUES(1, NULL)");
        // Currently Presto returns current table schema for any previous snapshot access https://github.com/prestodb/presto/issues/23553
        // otherwise querying a tag uses the snapshot's schema https://iceberg.apache.org/docs/nightly/branching/#schema-selection-with-branches-and-tags
        assertQuery("SELECT * FROM test_table_references FOR SYSTEM_VERSION AS OF 'testTag' where id1=1", "VALUES(1, NULL)");
    }

    @Test
    public void testAllIcebergType()
    {
        String tmpTableName = "test_vector_reader_all_type";
        try {
            assertUpdate(format("" +
                    "CREATE TABLE %s ( " +
                    "   c_boolean BOOLEAN, " +
                    "   c_int INT," +
                    "   c_bigint BIGINT, " +
                    "   c_double DOUBLE, " +
                    "   c_real REAL, " +
                    "   c_date DATE, " +
                    "   c_timestamp TIMESTAMP, " +
                    "   c_varchar VARCHAR, " +
                    "   c_varbinary VARBINARY, " +
                    "   c_array ARRAY(BIGINT), " +
                    "   c_map MAP(VARCHAR, INT), " +
                    "   c_row ROW(a INT, b VARCHAR) " +
                    ") WITH (format = 'PARQUET')", tmpTableName));

            assertUpdate(format("" +
                    "INSERT INTO %s " +
                    "SELECT c_boolean, c_int, c_bigint, c_double, c_real, c_date, c_timestamp, c_varchar, c_varbinary, c_array, c_map, c_row " +
                    "FROM ( " +
                    "  VALUES " +
                    "    (null, null, null, null, null, null, null, null, null, null, null, null), " +
                    "    (true, INT '1245', BIGINT '1', DOUBLE '2.2', REAL '-24.124', DATE '2024-07-29', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), to_ieee754_64(1), sequence(0, 10), MAP(ARRAY['aaa', 'bbbb'], ARRAY[1, 2]), CAST(ROW(1, 'AAA') AS ROW(a INT, b VARCHAR)))," +
                    "    (false, INT '-1245', BIGINT '-1', DOUBLE '2.3', REAL '243215.435', DATE '2024-07-29', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), to_ieee754_64(4), sequence(30, 35), MAP(ARRAY['ccc', 'bbbb'], ARRAY[-1, -2]), CAST(ROW(-1, 'AAA') AS ROW(a INT, b VARCHAR))) " +
                    ") AS x (c_boolean, c_int, c_bigint, c_double, c_real, c_date, c_timestamp, c_varchar, c_varbinary, c_array, c_map, c_row)", tmpTableName), 3);

            Session decimalVectorReaderEnabled = decimalVectorReaderEnabledSession(true);
            Session decimalVectorReaderDisable = decimalVectorReaderEnabledSession(false);
            assertQueryWithSameQueryRunner(decimalVectorReaderEnabled, "SELECT * FROM " + tmpTableName, decimalVectorReaderDisable);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tmpTableName);
        }
    }

    @Test
    public void testExpireSnapshotWithDeletedEntries()
    {
        try {
            assertUpdate("create table test_expire_snapshot_with_deleted_entry (a int, b varchar) with (partitioning = ARRAY['a'])");
            assertUpdate("insert into test_expire_snapshot_with_deleted_entry values(1, '1001'), (1, '1002'), (2, '2001'), (2, '2002')", 4);
            Table table = loadTable("test_expire_snapshot_with_deleted_entry");
            long snapshotId1 = table.currentSnapshot().snapshotId();

            // Execute metadata deletion which delete whole files from table metadata
            assertUpdate("delete from test_expire_snapshot_with_deleted_entry where a = 1", 2);
            table = loadTable("test_expire_snapshot_with_deleted_entry");
            long snapshotId2 = table.currentSnapshot().snapshotId();

            assertUpdate("insert into test_expire_snapshot_with_deleted_entry values(1, '1003'), (2, '2003'), (3, '3003')", 3);
            table = loadTable("test_expire_snapshot_with_deleted_entry");
            long snapshotId3 = table.currentSnapshot().snapshotId();

            assertQuery("select snapshot_id from \"test_expire_snapshot_with_deleted_entry$snapshots\"", "values " + snapshotId1 + ", " + snapshotId2 + ", " + snapshotId3);

            // Expire `snapshotId2` which contains a DELETED entry to delete a data file which is still referenced by `snapshotId1`
            assertUpdate(format("call iceberg.system.expire_snapshots(schema => '%s', table_name => '%s', snapshot_ids => ARRAY[%d])", "tpch", "test_expire_snapshot_with_deleted_entry", snapshotId2));
            assertQuery("select snapshot_id from \"test_expire_snapshot_with_deleted_entry$snapshots\"", "values " + snapshotId1 + ", " + snapshotId3);

            // Execute time travel query successfully
            assertQuery("select * from test_expire_snapshot_with_deleted_entry for version as of " + snapshotId1, "values(1, '1001'), (1, '1002'), (2, '2001'), (2, '2002')");
            assertQuery("select * from test_expire_snapshot_with_deleted_entry for version as of " + snapshotId3, "values(1, '1003'), (2, '2001'), (2, '2002'), (2, '2003'), (3, '3003')");
        }
        finally {
            assertUpdate("drop table if exists test_expire_snapshot_with_deleted_entry");
        }
    }

    private void testPathHiddenColumn()
    {
        assertEquals(computeActual("SELECT \"$path\", * FROM test_hidden_columns").getRowCount(), 2);

        // Fetch one of the file paths and use it in a filter
        String filePath = (String) computeActual("SELECT \"$path\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertEquals(
                computeActual(format("SELECT * from test_hidden_columns WHERE \"$path\"='%s'", filePath)).getRowCount(),
                1);

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", filePath))
                        .getOnlyValue(),
                1L);

        // Filter for $path that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", "non-existent-path"))
                        .getOnlyValue(),
                0L);
    }

    private void testDataSequenceNumberHiddenColumn()
    {
        assertEquals(computeActual("SELECT \"$data_sequence_number\", * FROM test_hidden_columns").getRowCount(), 2);

        // Fetch one of the data sequence numbers and use it in a filter
        Long dataSequenceNumber = (Long) computeActual("SELECT \"$data_sequence_number\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertEquals(
                computeActual(format("SELECT * from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber)).getRowCount(),
                1);

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber))
                        .getOnlyValue(),
                1L);

        // Filter for $data_sequence_number that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", 1000))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testHiddenColumns()
    {
        assertUpdate("DROP TABLE IF EXISTS test_hidden_columns");
        assertUpdate("CREATE TABLE test_hidden_columns AS SELECT * FROM tpch.tiny.region WHERE regionkey=0", 1);
        assertUpdate("INSERT INTO test_hidden_columns SELECT * FROM tpch.tiny.region WHERE regionkey=1", 1);

        testPathHiddenColumn();
        testDataSequenceNumberHiddenColumn();
    }

    @DataProvider(name = "pushdownFilterEnabled")
    public Object[][] pushdownFilterEnabledProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testFilterWithRemainingPredicate(boolean pushdownFilterEnabled)
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", PUSHDOWN_FILTER_ENABLED, Boolean.toString(pushdownFilterEnabled))
                .build();
        int rowCount = 100;
        String pairs = Joiner.on(", ")
                .join(IntStream.range(0, rowCount)
                        .map(idx -> idx * 2)
                        .mapToObj(idx -> "(" + idx + ", " + (idx + 1) + ")")
                        .iterator());

        assertQuerySucceeds("CREATE TABLE test_filterstats_remaining_predicate(i int, j int)");
        assertUpdate(format("INSERT INTO test_filterstats_remaining_predicate VALUES %s", pairs), rowCount);
        assertQuerySucceeds("ANALYZE test_filterstats_remaining_predicate");
        @Language("SQL") String query = "SELECT * FROM test_filterstats_remaining_predicate WHERE (i = 10 AND j = 11) OR (i = 20 AND j = 21)";
        if (pushdownFilterEnabled) {
            assertPlan(session, query,
                    output(
                            exchange(
                                    tableScan("test_filterstats_remaining_predicate")
                                            .withOutputRowCount(1))));
        }
        else {
            assertPlan(session, query,
                    anyTree(
                            filter(tableScan("test_filterstats_remaining_predicate")
                                    .withOutputRowCount(100))
                                    .withOutputRowCount(1)));
        }
        assertQuerySucceeds("DROP TABLE test_filterstats_remaining_predicate");
    }

    public void testStatisticsFileCache()
            throws Exception
    {
        assertQuerySucceeds("CREATE TABLE test_statistics_file_cache(i int)");
        assertUpdate("INSERT INTO test_statistics_file_cache VALUES 1, 2, 3, 4, 5", 5);
        assertQuerySucceeds("ANALYZE test_statistics_file_cache");
        Session session = Session.builder(getSession())
                .setTransactionId(getQueryRunner().getTransactionManager().beginTransaction(false))
                .build();
        Optional<TableHandle> handle = MetadataUtil.getOptionalTableHandle(session,
                getQueryRunner().getTransactionManager(),
                QualifiedObjectName.valueOf(session.getCatalog().get(), session.getSchema().get(), "test_statistics_file_cache"),
                Optional.empty());
        CatalogMetadata catalogMetadata = getQueryRunner().getTransactionManager()
                .getCatalogMetadata(session.getTransactionId().get(), handle.get().getConnectorId());
        // There isn't an easy way to access the cache internally, so use some reflection to grab it
        Field delegate = ClassLoaderSafeConnectorMetadata.class.getDeclaredField("delegate");
        delegate.setAccessible(true);
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) delegate.get(catalogMetadata.getMetadataFor(handle.get().getConnectorId()));
        CacheStats initial = metadata.statisticsFileCache.stats();
        assertEquals(metadata.statisticsFileCache.stats().minus(initial).hitCount(), 0);
        TableStatistics stats = getTableStats("test_statistics_file_cache", Optional.empty(), getSession(), Optional.of(ImmutableList.of("i")));
        assertEquals(stats.getRowCount().getValue(), 5);
        assertEquals(metadata.statisticsFileCache.stats().minus(initial).missCount(), 1);
        getTableStats("test_statistics_file_cache", Optional.empty(), getSession(), Optional.of(ImmutableList.of("i")));
        assertEquals(metadata.statisticsFileCache.stats().minus(initial).missCount(), 1);
        assertEquals(metadata.statisticsFileCache.stats().minus(initial).hitCount(), 1);
        getQueryRunner().execute("DROP TABLE test_statistics_file_cache");
    }

    @DataProvider(name = "parquetVersions")
    public Object[][] parquetVersionsDataProvider()
    {
        return new Object[][] {
                {PARQUET_1_0},
                {PARQUET_2_0},
        };
    }

    @Test(dataProvider = "parquetVersions")
    public void testBatchReadOnTimeType(WriterVersion writerVersion)
    {
        Session parquetVersionSession = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "parquet_writer_version", writerVersion.toString())
                .build();
        assertQuerySucceeds(parquetVersionSession, "CREATE TABLE time_batch_read(i int, t time)");
        assertUpdate(parquetVersionSession, "INSERT INTO time_batch_read VALUES (0, time '1:00:00.000'), (1, time '1:00:00.000'), (2, time '1:00:00.000')", 3);
        Session disabledBatchRead = Session.builder(parquetVersionSession)
                .setCatalogSessionProperty("iceberg", PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, "false")
                .build();
        Session enabledBatchRead = Session.builder(parquetVersionSession)
                .setCatalogSessionProperty("iceberg", PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, "true")
                .build();
        @Language("SQL") String query = "SELECT t FROM time_batch_read ORDER BY i LIMIT 1";
        MaterializedResult disabledResult = getQueryRunner().execute(disabledBatchRead, query);
        MaterializedResult enabledResult = getQueryRunner().execute(enabledBatchRead, query);
        assertEquals(disabledResult, enabledResult);
        assertQuerySucceeds("DROP TABLE time_batch_read");
    }

    private void testCheckDeleteFiles(Table icebergTable, int expectedSize, List<FileContent> expectedFileContent)
    {
        // check delete file list
        TableScan tableScan = icebergTable.newScan().useSnapshot(icebergTable.currentSnapshot().snapshotId());
        Iterator<FileScanTask> iterator = TableScanUtil.splitFiles(tableScan.planFiles(), tableScan.targetSplitSize()).iterator();
        List<DeleteFile> deleteFiles = new ArrayList<>();
        while (iterator.hasNext()) {
            FileScanTask task = iterator.next();
            List<org.apache.iceberg.DeleteFile> deletes = task.deletes();
            deletes.forEach(delete -> deleteFiles.add(DeleteFile.fromIceberg(delete)));
        }

        assertEquals(deleteFiles.size(), expectedSize);
        List<FileContent> fileContents = deleteFiles.stream().map(DeleteFile::content).sorted().collect(Collectors.toList());
        assertEquals(fileContents, expectedFileContent);
    }

    private void writePositionDeleteToNationTable(Table icebergTable, String dataFilePath, long deletePos)
            throws IOException
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        File metastoreDir = getIcebergDataDirectoryPath(dataDirectory, catalogType.name(), new IcebergConfig().getFileFormat(), false).toFile();
        org.apache.hadoop.fs.Path metadataDir = new org.apache.hadoop.fs.Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + UUID.randomUUID();
        FileSystem fs = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), metadataDir);
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(metadataDir, deleteFileName);
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(HadoopOutputFile.fromPath(path, fs))
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(icebergTable)
                .overwrite()
                .rowSchema(icebergTable.schema())
                .withSpec(PartitionSpec.unpartitioned())
                .buildPositionWriter();

        PositionDelete<Record> positionDelete = PositionDelete.create();
        PositionDelete<Record> record = positionDelete.set(dataFilePath, deletePos, GenericRecord.create(icebergTable.schema()));
        try (Closeable ignored = writer) {
            writer.write(record);
        }

        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    private void writeEqualityDeleteToNationTable(Table icebergTable, Map<String, Object> overwriteValues)
            throws Exception
    {
        writeEqualityDeleteToNationTable(icebergTable, overwriteValues, Collections.emptyMap());
    }

    private void writeEqualityDeleteToNationTable(Table icebergTable, Map<String, Object> overwriteValues, Map<String, Object> partitionValues)
            throws Exception
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        File metastoreDir = getIcebergDataDirectoryPath(dataDirectory, catalogType.name(), new IcebergConfig().getFileFormat(), false).toFile();
        org.apache.hadoop.fs.Path metadataDir = new org.apache.hadoop.fs.Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + UUID.randomUUID();
        FileSystem fs = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), metadataDir);
        Schema deleteRowSchema = icebergTable.schema().select(overwriteValues.keySet());
        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(metadataDir, deleteFileName), fs))
                .forTable(icebergTable)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .equalityFieldIds(deleteRowSchema.columns().stream().map(Types.NestedField::fieldId).collect(Collectors.toList()))
                .overwrite();

        if (!partitionValues.isEmpty()) {
            GenericRecord partitionData = GenericRecord.create(icebergTable.spec().partitionType());
            writerBuilder.withPartition(partitionData.copy(partitionValues));
        }

        EqualityDeleteWriter<Object> writer = writerBuilder.buildEqualityWriter();

        Record dataDelete = GenericRecord.create(deleteRowSchema);
        try (Closeable ignored = writer) {
            writer.write(dataDelete.copy(overwriteValues));
        }
        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    public static HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(),
                hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
    }

    private Table updateTable(String tableName)
    {
        BaseTable table = (BaseTable) loadTable(tableName);
        TableOperations operations = table.operations();
        TableMetadata currentMetadata = operations.current();
        operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(2));

        return table;
    }

    protected Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(catalogType.getCatalogImpl(), "test-hive", getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of("tpch", tableName));
    }

    protected Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    protected File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        switch (catalogType) {
            case HIVE:
            case HADOOP:
            case NESSIE:
                return getIcebergDataDirectoryPath(dataDirectory, catalogType.name(), new IcebergConfig().getFileFormat(), false).toFile();
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported Presto Iceberg catalog type " + catalogType);
    }

    private Session sessionForTimezone(String zoneId, boolean legacyTimestamp)
    {
        SessionBuilder sessionBuilder = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, String.valueOf(legacyTimestamp));
        if (legacyTimestamp) {
            sessionBuilder.setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId));
        }
        return sessionBuilder.build();
    }

    private void testWithAllFileFormats(Session session, BiConsumer<Session, FileFormat> test)
    {
        test.accept(session, FileFormat.PARQUET);
        test.accept(session, FileFormat.ORC);
    }

    private void assertHasDataFiles(Snapshot snapshot, int dataFilesCount)
    {
        Map<String, String> map = snapshot.summary();
        int totalDataFiles = Integer.valueOf(map.get(TOTAL_DATA_FILES_PROP));
        assertEquals(totalDataFiles, dataFilesCount);
    }

    private void assertHasDeleteFiles(Snapshot snapshot, int deleteFilesCount)
    {
        Map<String, String> map = snapshot.summary();
        int totalDeleteFiles = Integer.valueOf(map.get(TOTAL_DELETE_FILES_PROP));
        assertEquals(totalDeleteFiles, deleteFilesCount);
    }
}
