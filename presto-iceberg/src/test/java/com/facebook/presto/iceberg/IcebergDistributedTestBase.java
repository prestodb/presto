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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.hive.BaseHiveColumnHandle;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
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
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.ConnectorHistogram;
import com.facebook.presto.spi.statistics.DoubleRange;
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
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveCommonSessionProperties.PARQUET_BATCH_READ_OPTIMIZATION_ENABLED;
import static com.facebook.presto.iceberg.FileContent.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.IcebergSessionProperties.DELETE_AS_JOIN_REWRITE_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.DELETE_AS_JOIN_REWRITE_MAX_DELETE_COLUMNS;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static com.facebook.presto.type.DecimalParametricType.DECIMAL;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class IcebergDistributedTestBase
        extends AbstractTestQueryFramework
{
    private static final String METADATA_FILE_EXTENSION = ".metadata.json";
    protected final CatalogType catalogType;
    protected final Map<String, String> extraConnectorProperties;
    protected IcebergQueryRunner icebergQueryRunner;

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
        this.icebergQueryRunner = IcebergQueryRunner.builder()
                .setCatalogType(catalogType)
                .setExtraConnectorProperties(extraConnectorProperties)
                .build();
        return icebergQueryRunner.getQueryRunner();
    }

    @Test
    public void testDeleteOnV1Table()
    {
        // Test delete all rows
        long totalCount = (long) getQueryRunner().execute("CREATE TABLE test_delete with (\"format-version\" = '1') as select * from lineitem")
                .getOnlyValue();
        assertUpdate("DELETE FROM test_delete", totalCount);
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 0L);
        assertQuerySucceeds("DROP TABLE test_delete");

        // Test delete whole partitions identified by one partition column
        totalCount = (long) getQueryRunner().execute("CREATE TABLE test_partitioned_drop WITH (\"format-version\" = '1', partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem")
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
        totalCount = (long) getQueryRunner().execute("CREATE TABLE test_partitioned_drop WITH (\"format-version\" = '1', partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem")
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
        assertUpdate("CREATE TABLE test_partitioned_drop WITH (\"format-version\" = '1', partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem", totalCount);
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
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (\"format-version\" = '2', \"write.delete.mode\" = 'merge-on-read')");
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
            assertQuerySucceeds(session, format("create table test_partition_columns(a bigint, b timestamp) with (partitioning = ARRAY['b'], \"write.format.default\" = '%s')", fileFormat.name()));
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
    public void testCreateTableWithCustomLocation()
            throws IOException
    {
        String tableName = "test_table_with_custom_location";
        URI tableTargetURI = createTempDirectory(tableName).toUri();
        try {
            assertQuerySucceeds(format("create table %s (a int, b varchar)" +
                    " with (location = '%s')", tableName, tableTargetURI.toString()));
            assertUpdate(format("insert into %s values(1, '1001'), (2, '1002')", tableName), 2);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002')");
            TableMetadata tableMetadata = ((BaseTable) loadTable(tableName)).operations().current();
            assertEquals(URI.create(tableMetadata.location() + File.separator), tableTargetURI);
        }
        finally {
            assertUpdate("drop table if exists " + tableName);
        }
    }

    @Test
    protected void testCreateTableAndValidateIcebergTableName()
    {
        String tableName = "test_create_table_for_validate_name";
        Session session = getSession();
        assertUpdate(session, format("CREATE TABLE %s (col1 INTEGER, aDate DATE)", tableName));
        Table icebergTable = loadTable(tableName);

        String catalog = session.getCatalog().get();
        String schemaName = session.getSchema().get();
        assertEquals(icebergTable.name(), catalog + "." + schemaName + "." + tableName);

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
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
            assertUpdate("CREATE TABLE test_stats_with_column_limits (column_0 int, column_1 varchar, " + columns + comma + "column_10001 varchar) with(\"write.metadata.metrics.max-inferred-column-defaults\" = " + columnCount + ")");
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

    private TableStatistics getTableStats(String name, Optional<Long> snapshot, Session session)
    {
        return getTableStats(name, snapshot, session, Optional.empty());
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
        TableStatistics tableStatistics = metadata.getTableStatistics(metadataSession,
                handle,
                new ArrayList<>(columns
                        .map(columnSet -> Maps.filterKeys(resolver.getColumnHandles(handle), columnSet::contains))
                        .orElseGet(() -> resolver.getColumnHandles(handle)).values()),
                Constraint.alwaysTrue());

        getQueryRunner().getTransactionManager().asyncAbort(transactionId);
        return tableStatistics;
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
        assertUpdate("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
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
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (\"write.format.default\" = '" + fileFormat + "', partitioning=ARRAY['a'])");
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
        assertUpdate(session, "CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate(session, "CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate(session, "CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
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
        assertUpdate(session, "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['nationkey'], \"write.format.default\" = '" + fileFormat + "') " + " AS SELECT * FROM tpch.tiny.nation", 25);
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
        assertUpdate("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "', partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
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
        assertUpdate("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "', partitioning = ARRAY['bucket(nationkey,100)']) AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
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
        assertUpdate("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
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
        assertUpdate("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
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
        assertUpdate(session, "CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
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
        assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (\"write.format.default\" = '" + fileFormat + "')");
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

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeletesWithDataSequenceNumber(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        String tableName2 = "test_v2_row_delete_2_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int, data varchar) WITH (\"write.format.default\" = '" + fileFormat + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertUpdate("CREATE TABLE " + tableName2 + "(id int, data varchar) WITH (\"write.format.default\" = '" + fileFormat + "')");
        assertUpdate("INSERT INTO " + tableName2 + " VALUES (1, 'a')", 1);

        Table icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("id", 1));

        Table icebergTable2 = updateTable(tableName2);
        writeEqualityDeleteToNationTable(icebergTable2, ImmutableMap.of("id", 1));

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'b'), (2, 'a'), (3, 'a')", 3);
        assertUpdate("INSERT INTO " + tableName2 + " VALUES (1, 'b'), (2, 'a'), (3, 'a')", 3);

        assertQuery(session, "SELECT * FROM " + tableName, "VALUES (1, 'b'), (2, 'a'), (3, 'a')");

        assertQuery(session, "SELECT \"$data_sequence_number\", * FROM " + tableName, "VALUES (3, 1, 'b'), (3, 2, 'a'), (3, 3, 'a')");

        assertQuery(session, "SELECT a.\"$data_sequence_number\", b.\"$data_sequence_number\" from " + tableName + " as a, " + tableName2 + " as b where a.id = b.id",
                "VALUES (3, 3), (3, 3), (3, 3)");
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
    public void testWithSortOrder()
            throws IOException
    {
        String tableName = "test_create_sorted_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int, emp_name varchar) WITH (sorted_by = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'EEEE'), (3, 'CCCC'), (1, 'AAAA'), (2, 'BBBB'), (4,'DDDD')", 5);
        for (Object filePath : computeActual("SELECT file_path from \"" + tableName + "$files\"").getOnlyColumnAsSet()) {
            assertTrue(isFileSorted(String.valueOf(filePath), "id", "ASC"));
        }
    }

    @Test
    public void testWithDescSortOrder()
            throws IOException
    {
        String tableName = "test_create_sorted_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int, emp_name varchar) WITH (sorted_by = ARRAY['id DESC'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'EEEE'), (3, 'CCCC'), (1, 'AAAA'), (2, 'BBBB'), (4,'DDDD')", 5);
        for (Object filePath : computeActual("SELECT file_path from \"" + tableName + "$files\"").getOnlyColumnAsSet()) {
            assertTrue(isFileSorted(String.valueOf(filePath), "id", "DESC"));
        }
    }

    @Test
    public void testWithoutSortOrder()
            throws IOException
    {
        String tableName = "test_create_unsorted_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int, emp_name varchar)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'EEEE'), (3, 'CCCC'), (5, 'AAAA'), (2, 'BBBB'), (4,'DDDD')", 5);
        for (Object filePath : computeActual("SELECT file_path from \"" + tableName + "$files\"").getOnlyColumnAsSet()) {
            assertFalse(isFileSorted(String.valueOf(filePath), "id", ""));
        }
    }

    public boolean isFileSorted(String path, String sortColumnName, String sortOrder)
            throws IOException
    {
        Path filePath = new Path(path);
        Configuration configuration = getHdfsEnvironment().getConfiguration(new HdfsContext(SESSION), filePath);
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(path))
                .withConf(configuration)
                .build()) {
            Group record;
            ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, filePath);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            Double previousValue = null;
            while ((record = reader.read()) != null) {
                for (int i = 0; i < record.getType().getFieldCount(); i++) {
                    String columnName = schema.getFieldName(i);
                    if (columnName.equals(sortColumnName)) {
                        Double currentValue = Double.parseDouble(record.getValueToString(i, i));
                        if (previousValue != null) {
                            boolean valueNotSorted = ("ASC".equals(sortOrder) || "".equals(sortOrder))
                                    ? currentValue.compareTo(previousValue) < 0
                                    : currentValue.compareTo(previousValue) > 0;

                            if (valueNotSorted) {
                                return false;
                            }
                        }
                        previousValue = currentValue;
                    }
                }
            }
        }
        return true;
    }

    @Test
    public void testMetadataDeleteOnUnPartitionedTableWithDeleteFiles()
    {
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (\"format-version\" = '2', \"write.delete.mode\" = 'merge-on-read')");
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

    @DataProvider(name = "validHistogramTypes")
    public Object[][] validHistogramTypesDataProvider()
    {
        return new Object[][] {
                // types not supported in Iceberg connector, but that histogram could support
                // {TINYINT, new String[]{"1", "2", "10"}},
                // {SMALLINT, new String[]{"1", "2", "10"}},
                // {TIMESTAMP_WITH_TIME_ZONE, new String[]{"now() + interval '1' hour", "now() + interval '2' hour"}},
                // iceberg stores microsecond precision but presto calculates on millisecond precision
                // need a fix to properly convert for the optimizer.
                // {TIMESTAMP, new String[] {"localtimestamp + interval '1' hour", "localtimestamp + interval '2' hour"}},
                // {TIME, new String[] {"localtime", "localtime + interval '1' hour"}},
                // supported types
                {INTEGER, new String[] {"1", "5", "9"}},
                {BIGINT, new String[] {"2", "4", "6"}},
                {DOUBLE, new String[] {"1.0", "3.1", "4.6"}},
                // short decimal
                {DECIMAL.createType(ImmutableList.of(TypeParameter.of(2L), TypeParameter.of(1L))), new String[] {"0.0", "3.0", "4.0"}},
                // long decimal
                {DECIMAL.createType(ImmutableList.of(TypeParameter.of(38L), TypeParameter.of(1L))), new String[] {"0.0", "3.0", "4.0"}},
                {DATE, new String[] {"date '2024-01-01'", "date '2024-03-30'", "date '2024-05-30'"}},
                {REAL, new String[] {"1.0", "2.0", "3.0"}},
        };
    }

    /**
     * Verifies that the histogram is returned after ANALYZE for a variety of types
     */
    @Test(dataProvider = "validHistogramTypes")
    public void testHistogramStorage(Type type, Object[] values)
    {
        try {
            Session session = Session.builder(getSession())
                    .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                    .build();
            assertQuerySucceeds("DROP TABLE IF EXISTS create_histograms");
            assertQuerySucceeds(String.format("CREATE TABLE create_histograms (c %s)", type.getDisplayName()));
            assertQuerySucceeds(String.format("INSERT INTO create_histograms VALUES %s", Joiner.on(", ").join(values)));
            assertQuerySucceeds(session, "ANALYZE create_histograms");
            TableStatistics tableStatistics = getTableStats("create_histograms");
            Map<String, IcebergColumnHandle> nameToHandle = tableStatistics.getColumnStatistics().keySet()
                    .stream().map(IcebergColumnHandle.class::cast)
                    .collect(Collectors.toMap(BaseHiveColumnHandle::getName, identity()));
            assertNotNull(nameToHandle.get("c"));
            IcebergColumnHandle handle = nameToHandle.get("c");
            ColumnStatistics statistics = tableStatistics.getColumnStatistics().get(handle);
            assertTrue(statistics.getHistogram().isPresent());
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS create_histograms");
        }
    }

    @Test
    public void testMetadataDeleteOnPartitionedTableWithDeleteFiles()
    {
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar) WITH (\"format-version\" = '2', \"write.delete.mode\" = 'merge-on-read', partitioning = ARRAY['a'])");
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
            assertUpdate("CREATE TABLE " + tableName + " (a INTEGER, b VARCHAR) WITH (\"format-version\" = '2', \"write.delete.mode\" = 'merge-on-read')");

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
            assertUpdate("CREATE TABLE " + tableName + " (a INTEGER, b VARCHAR) WITH (\"format-version\" = '2', \"write.delete.mode\" = 'merge-on-read', partitioning = ARRAY['a'])");
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
                    " WITH (\"write.metadata.previous-versions-max\" = 1, \"write.metadata.delete-after-commit.enabled\" = true)");

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

            FileSystem fileSystem = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), new Path(settingTable.location()));

            // Table `test_table_with_setting_properties`'s all existing metadata files count is 2
            FileStatus[] settingTableFiles = fileSystem.listStatus(new Path(settingTable.location(), "metadata"), name -> name.getName().contains(METADATA_FILE_EXTENSION));
            assertEquals(settingTableFiles.length, 2);

            // Table `test_table_with_default_setting_properties`'s all existing metadata files count is 6
            FileStatus[] defaultTableFiles = fileSystem.listStatus(new Path(defaultTable.location(), "metadata"), name -> name.getName().contains(METADATA_FILE_EXTENSION));
            assertEquals(defaultTableFiles.length, 6);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + settingTableName);
        }
    }

    @DataProvider(name = "batchReadEnabled")
    public Object[] batchReadEnabledReader()
    {
        return new Object[] {true, false};
    }

    private Session batchReadEnabledEnabledSession(boolean batchReadEnabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, String.valueOf(batchReadEnabled))
                .build();
    }

    @Test(dataProvider = "batchReadEnabled")
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

            Session session = batchReadEnabledEnabledSession(decimalVectorReaderEnabled);
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
                    "   c_uuid UUID, " +
                    "   c_array ARRAY(BIGINT), " +
                    "   c_map MAP(VARCHAR, INT), " +
                    "   c_row ROW(a INT, b VARCHAR) " +
                    ") WITH (\"write.format.default\" = 'PARQUET')", tmpTableName));

            assertUpdate(format("" +
                    "INSERT INTO %s " +
                    "SELECT c_boolean, c_int, c_bigint, c_double, c_real, c_date, c_timestamp, c_varchar, c_varbinary, c_uuid, c_array, c_map, c_row " +
                    "FROM ( " +
                    "  VALUES " +
                    "    (null, null, null, null, null, null, null, null, null, null, null, null, null), " +
                    "    (true, INT '1245', BIGINT '1', DOUBLE '2.2', REAL '-24.124', DATE '2024-07-29', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), to_ieee754_64(1), CAST('4ae71336-e44b-39bf-b9d2-752e234818a5' as UUID), sequence(0, 10), MAP(ARRAY['aaa', 'bbbb'], ARRAY[1, 2]), CAST(ROW(1, 'AAA') AS ROW(a INT, b VARCHAR)))," +
                    "    (false, INT '-1245', BIGINT '-1', DOUBLE '2.3', REAL '243215.435', DATE '2024-07-29', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), to_ieee754_64(4), CAST('4ae71336-e44b-39bf-b9d2-752e234818a5' as UUID), sequence(30, 35), MAP(ARRAY['ccc', 'bbbb'], ARRAY[-1, -2]), CAST(ROW(-1, 'AAA') AS ROW(a INT, b VARCHAR))) " +
                    ") AS x (c_boolean, c_int, c_bigint, c_double, c_real, c_date, c_timestamp, c_varchar, c_varbinary, c_uuid, c_array, c_map, c_row)", tmpTableName), 3);

            Session decimalVectorReaderEnabled = batchReadEnabledEnabledSession(true);
            Session decimalVectorReaderDisable = batchReadEnabledEnabledSession(false);
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

    @Test
    public void testDeletedHiddenColumn()
    {
        assertUpdate("DROP TABLE IF EXISTS test_deleted");
        assertUpdate("CREATE TABLE test_deleted AS SELECT * FROM tpch.tiny.region WHERE regionkey=0", 1);
        assertUpdate("INSERT INTO test_deleted SELECT * FROM tpch.tiny.region WHERE regionkey=1", 1);

        assertQuery("SELECT \"$deleted\" FROM test_deleted", format("VALUES %s, %s", "false", "false"));

        assertUpdate("DELETE FROM test_deleted WHERE regionkey=1", 1);
        assertEquals(computeActual("SELECT * FROM test_deleted").getRowCount(), 1);
        assertQuery("SELECT \"$deleted\" FROM test_deleted ORDER BY \"$deleted\"", format("VALUES %s, %s", "false", "true"));
    }

    @Test
    public void testDeleteFilePathHiddenColumn()
    {
        assertUpdate("DROP TABLE IF EXISTS test_delete_file_path");
        assertUpdate("CREATE TABLE test_delete_file_path AS SELECT * FROM tpch.tiny.region WHERE regionkey=0", 1);
        assertUpdate("INSERT INTO test_delete_file_path SELECT * FROM tpch.tiny.region WHERE regionkey=1", 1);

        assertQuery("SELECT \"$delete_file_path\" FROM test_delete_file_path", format("VALUES %s, %s", "NULL", "NULL"));

        assertUpdate("DELETE FROM test_delete_file_path WHERE regionkey=1", 1);
        assertEquals(computeActual("SELECT * FROM test_delete_file_path").getRowCount(), 1);
        assertEquals(computeActual("SELECT \"$delete_file_path\" FROM test_delete_file_path").getRowCount(), 2);

        assertUpdate("DELETE FROM test_delete_file_path WHERE regionkey=0", 1);
        computeActual("SELECT \"$delete_file_path\" FROM test_delete_file_path").getMaterializedRows().forEach(row -> {
            assertEquals(row.getFieldCount(), 1);
            assertNotNull(row.getField(0));
        });
    }

    @Test(dataProvider = "equalityDeleteOptions")
    public void testEqualityDeletesWithDeletedHiddenColumn(String fileFormat, boolean joinRewriteEnabled)
            throws Exception
    {
        Session session = deleteAsJoinEnabled(joinRewriteEnabled);
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int, data varchar) WITH (\"write.format.default\" = '" + fileFormat + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        Table icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("id", 1));

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'b'), (2, 'a'), (3, 'a')", 3);

        assertQuery(session, "SELECT * FROM " + tableName, "VALUES (1, 'b'), (2, 'a'), (3, 'a')");

        assertQuery(session, "SELECT \"$deleted\", * FROM " + tableName,
                "VALUES (true, 1, 'a'), (false, 1, 'b'), (false, 2, 'a'), (false, 3, 'a')");
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

        TransactionId transactionId = getQueryRunner().getTransactionManager().beginTransaction(false);
        Session session = Session.builder(getSession())
                .setTransactionId(transactionId)
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

        getQueryRunner().getTransactionManager().asyncAbort(transactionId);
        getQueryRunner().execute("DROP TABLE test_statistics_file_cache");
    }

    @Test(dataProvider = "batchReadEnabled")
    public void testUuidRoundTrip(boolean batchReadEnabled)
    {
        Session session = batchReadEnabledEnabledSession(batchReadEnabled);
        try {
            assertQuerySucceeds("CREATE TABLE uuid_roundtrip(u uuid)");
            UUID uuid = UUID.fromString("11111111-2222-3333-4444-555555555555");
            assertUpdate(format("INSERT INTO uuid_roundtrip VALUES CAST('%s' as uuid)", uuid), 1);
            assertQuery(session, "SELECT CAST(u as varchar) FROM uuid_roundtrip", format("VALUES '%s'", uuid));
        }
        finally {
            assertQuerySucceeds("DROP TABLE uuid_roundtrip");
        }
    }

    @Test(dataProvider = "batchReadEnabled")
    public void testUuidFilters(boolean batchReadEnabled)
    {
        Session session = batchReadEnabledEnabledSession(batchReadEnabled);
        try {
            int uuidCount = 100;
            assertQuerySucceeds("CREATE TABLE uuid_filters(u uuid)");
            List<UUID> uuids = IntStream.range(0, uuidCount)
                    .mapToObj(idx -> {
                        ByteBuffer buf = ByteBuffer.allocate(16);
                        if (idx % 2 == 0) {
                            buf.putLong(0L);
                            buf.putLong(idx);
                        }
                        else {
                            buf.putLong(idx);
                            buf.putLong(0L);
                        }
                        buf.flip();
                        return new UUID(buf.getLong(), buf.getLong());
                    })
                    .collect(Collectors.toList());
            // shuffle to make sure parquet metadata stats are updated properly even with
            // out-of-order values
            Collections.shuffle(uuids);
            assertUpdate(format("INSERT INTO uuid_filters VALUES %s",
                    Joiner.on(", ").join(uuids.stream().map(uuid -> format("CAST('%s' as uuid)", uuid)).iterator())), 100);
            assertQuery(session, format("SELECT CAST(u as varchar) FROM uuid_filters WHERE u = CAST('%s' as uuid)", uuids.get(0)), format("VALUES '%s'", uuids.get(0)));

            // sort so we can easily get lowest and highest
            uuids.sort(Comparator.naturalOrder());
            assertQuery(session, format("SELECT COUNT(*) FROM uuid_filters WHERE u >= CAST('%s' as uuid)", uuids.get(0)), format("VALUES %d", uuidCount));
            assertQuery(session, format("SELECT COUNT(*) FROM uuid_filters WHERE u > CAST('%s' as uuid)", uuids.get(0)), format("VALUES %d", uuidCount - 1));
            assertQuery(session, format("SELECT COUNT(*) FROM uuid_filters WHERE u <= CAST('%s' as uuid)", uuids.get(uuidCount - 1)), format("VALUES %d", uuidCount));
            assertQuery(session, format("SELECT COUNT(*) FROM uuid_filters WHERE u < CAST('%s' as uuid)", uuids.get(uuidCount - 1)), format("VALUES %d", uuidCount - 1));
            assertQuery(session, format("SELECT COUNT(*) FROM uuid_filters WHERE u < CAST('%s' as uuid)", uuids.get(50)), format("VALUES %d", 50));
            assertQuery(session, format("SELECT COUNT(*) FROM uuid_filters WHERE u <= CAST('%s' as uuid)", uuids.get(50)), format("VALUES %d", 51));
        }
        finally {
            assertQuerySucceeds("DROP TABLE uuid_filters");
        }
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

    public void testAllNullHistogramColumn()
    {
        try {
            Session session = Session.builder(getSession())
                    .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                    .build();
            assertQuerySucceeds("DROP TABLE IF EXISTS histogram_all_nulls");
            assertQuerySucceeds("CREATE TABLE histogram_all_nulls (c bigint)");
            TableStatistics stats = getTableStats("histogram_all_nulls", Optional.empty(), session);
            assertFalse(stats.getColumnStatistics().values().stream().findFirst().isPresent());
            assertUpdate("INSERT INTO histogram_all_nulls VALUES NULL, NULL, NULL, NULL, NULL", 5);
            stats = getTableStats("histogram_all_nulls", Optional.empty(), session);
            assertFalse(stats.getColumnStatistics().values().stream().findFirst()
                    .get().getHistogram().isPresent());
            assertQuerySucceeds(session, "ANALYZE histogram_all_nulls");
            stats = getTableStats("histogram_all_nulls", Optional.empty(), session);
            assertFalse(stats.getColumnStatistics().values().stream().findFirst()
                    .get().getHistogram().isPresent());
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS histogram_all_nulls");
        }
    }

    @Test(dataProvider = "validHistogramTypes")
    public void testHistogramShowStats(Type type, Object[] values)
    {
        try {
            Session session = Session.builder(getSession())
                    .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                    .build();
            assertQuerySucceeds("DROP TABLE IF EXISTS create_histograms");
            assertQuerySucceeds(String.format("CREATE TABLE show_histograms (c %s)", type.getDisplayName()));
            assertQuerySucceeds(String.format("INSERT INTO show_histograms VALUES %s", Joiner.on(", ").join(values)));
            assertQuerySucceeds(session, "ANALYZE show_histograms");
            TableStatistics tableStatistics = getTableStats("show_histograms", Optional.empty(), session, Optional.empty());
            Map<String, Optional<ConnectorHistogram>> histogramByColumnName = tableStatistics.getColumnStatistics()
                    .entrySet()
                    .stream()
                    .collect(toImmutableMap(
                            entry -> ((IcebergColumnHandle) entry.getKey()).getName(),
                            entry -> entry.getValue().getHistogram()));
            MaterializedResult stats = getQueryRunner().execute("SHOW STATS for show_histograms");
            stats.getMaterializedRows()
                    .forEach(row -> {
                        String name = (String) row.getField(0);
                        String histogram = (String) row.getField(7);
                        assertEquals(Optional.ofNullable(histogramByColumnName.get(name))
                                        .flatMap(identity())
                                        .map(Objects::toString).orElse(null),
                                histogram);
                    });
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS show_histograms");
        }
    }

    /**
     * Verifies that when the users opts-in to using histograms  that the
     * optimizer estimates reflect the actual dataset for a variety of filter
     * types (LTE, GT, EQ, NE) on a non-uniform data distribution
     */
    @Test
    public void testHistogramsUsedInOptimization()
    {
        Session histogramSession = Session.builder(getSession())
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                .build();
        // standard-normal distribution should have vastly different estimates than uniform at the tails (e.g. -3, +3)
        NormalDistribution dist = new NormalDistribution(0, 1);
        double[] values = dist.sample(1000);
        Arrays.sort(values);

        try {
            assertQuerySucceeds("DROP TABLE IF EXISTS histogram_validation");
            assertQuerySucceeds("CREATE TABLE histogram_validation (c double)");
            assertQuerySucceeds(String.format("INSERT INTO histogram_validation VALUES %s", Joiner.on(", ").join(Arrays.stream(values).iterator())));
            assertQuerySucceeds(histogramSession, "ANALYZE histogram_validation");
            Consumer<Double> assertFilters = (value) -> {
                // use Math.abs because if the value isn't found, the returned value of binary
                // search is (- insert index). The absolute value index tells us roughly how
                // many records would have been returned regardless of if the actual value is in the
                // dataset
                double estimatedRowCount = Math.abs(Arrays.binarySearch(values, value));
                assertPlan(histogramSession, "SELECT * FROM histogram_validation WHERE c <= " + value,
                        output(anyTree(tableScan("histogram_validation"))).withApproximateOutputRowCount(estimatedRowCount, 25));
                // check that inverse filter equals roughly the inverse number of rows
                assertPlan(histogramSession, "SELECT * FROM histogram_validation WHERE c > " + value,
                        output(anyTree(tableScan("histogram_validation"))).withApproximateOutputRowCount(Math.max(0.0, values.length - estimatedRowCount), 25));
                // having an exact random double value from the distribution exist more than once is exceedingly rare.
                // the histogram calculation should return 1 (and the inverse) in both situations
                assertPlan(histogramSession, "SELECT * FROM histogram_validation WHERE c = " + value,
                        output(anyTree(tableScan("histogram_validation"))).withApproximateOutputRowCount(1.0, 25));
                assertPlan(histogramSession, "SELECT * FROM histogram_validation WHERE c != " + value,
                        output(anyTree(tableScan("histogram_validation"))).withApproximateOutputRowCount(values.length - 1, 25));
            };

            assertFilters.accept(values[1]); // choose 1 greater than the min value
            assertFilters.accept(-2.0); // should be very unlikely to generate a distribution where all values > -2.0
            assertFilters.accept(-1.0);
            assertFilters.accept(0.0);
            assertFilters.accept(1.0);
            assertFilters.accept(2.0); // should be very unlikely to generate a distribution where all values < 2.0
            assertFilters.accept(values[values.length - 2]); // choose 1 less than the max value
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS histogram_validation");
        }
    }

    /**
     * Verifies that the data in the histogram matches the mins/maxs of the values
     * in the table when created
     */
    @Test(dataProvider = "validHistogramTypes")
    public void testHistogramReconstruction(Type type, Object[] values)
    {
        try {
            Session session = Session.builder(getSession())
                    .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                    .build();
            assertQuerySucceeds("DROP TABLE IF EXISTS verify_histograms");
            assertQuerySucceeds(String.format("CREATE TABLE verify_histograms (c %s)", type.getDisplayName()));
            assertQuerySucceeds(String.format("INSERT INTO verify_histograms VALUES %s", Joiner.on(", ").join(values)));
            assertQuerySucceeds(session, "ANALYZE verify_histograms");
            TableStatistics tableStatistics = getTableStats("verify_histograms", Optional.empty(), session, Optional.empty());
            Map<String, IcebergColumnHandle> nameToHandle = tableStatistics.getColumnStatistics().keySet()
                    .stream().map(IcebergColumnHandle.class::cast)
                    .collect(Collectors.toMap(BaseHiveColumnHandle::getName, identity()));
            assertNotNull(nameToHandle.get("c"));
            IcebergColumnHandle handle = nameToHandle.get("c");
            ColumnStatistics statistics = tableStatistics.getColumnStatistics().get(handle);
            ConnectorHistogram histogram = statistics.getHistogram().get();
            DoubleRange range = statistics.getRange().get();
            double min = range.getMin();
            double max = range.getMax();
            assertEquals(histogram.inverseCumulativeProbability(0.0).getValue(), min);
            assertEquals(histogram.inverseCumulativeProbability(1.0).getValue(), max);
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS verify_histograms");
        }
    }

    @Test
    public void testInformationSchemaQueries()
    {
        assertQuerySucceeds("CREATE SCHEMA ICEBERG.TEST_SCHEMA1");
        assertQuerySucceeds("CREATE SCHEMA ICEBERG.TEST_SCHEMA2");
        assertQuerySucceeds("CREATE TABLE ICEBERG.TEST_SCHEMA1.ICEBERG_T1(i int)");
        assertQuerySucceeds("CREATE TABLE ICEBERG.TEST_SCHEMA1.ICEBERG_T2(i int)");
        assertQuerySucceeds("CREATE TABLE ICEBERG.TEST_SCHEMA2.ICEBERG_T3(i int)");
        assertQuerySucceeds("CREATE TABLE ICEBERG.TEST_SCHEMA2.ICEBERG_T4(i int)");

        assertQuery("SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema ='test_schema1'", "VALUES 'iceberg_t1', 'iceberg_t2'");
        assertQuery("SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema ='test_schema2'", "VALUES 'iceberg_t3', 'iceberg_t4'");
        //query on non-existing schema
        assertQueryReturnsEmptyResult("SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema = 'NON_EXISTING_SCHEMA'");
        assertQueryReturnsEmptyResult("SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema = 'non_existing_schema'");

        assertQuerySucceeds("DROP TABLE ICEBERG.TEST_SCHEMA1.ICEBERG_T1");
        assertQuerySucceeds("DROP TABLE ICEBERG.TEST_SCHEMA1.ICEBERG_T2");
        assertQuerySucceeds("DROP TABLE ICEBERG.TEST_SCHEMA2.ICEBERG_T3");
        assertQuerySucceeds("DROP TABLE ICEBERG.TEST_SCHEMA2.ICEBERG_T4");
        assertQuerySucceeds("DROP SCHEMA ICEBERG.TEST_SCHEMA1");
        assertQuerySucceeds("DROP SCHEMA ICEBERG.TEST_SCHEMA2");
    }

    @Test
    public void testUpdateWithDuplicateValues()
    {
        String tableName = "test_update_duplicate_values_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int, column1 varchar(10), column2 varchar(10), column3 int)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 'a', 1), (2, 'b', 'b', 1), (3, 'c', 'c', 1)", 3);

        // update single row with duplicate values
        assertUpdate("UPDATE " + tableName + " SET column1 = CAST(1 as varchar), column2 = CAST(1 as varchar), column3 = 11 WHERE id = 1", 1);
        assertQuery("SELECT id, column1, column2, column3 FROM " + tableName, "VALUES (1, '1', '1', 11), (2, 'b', 'b', 1), (3, 'c', 'c', 1)");
    }

    @Test
    public void testUpdateWithPredicates()
    {
        String tableName = "test_update_predicates_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int, full_name varchar(20))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'aaaa'), (2, 'bbbb'), (3, 'cccc')", 3);
        // update single row on id
        assertUpdate("UPDATE " + tableName + " SET full_name = 'aaaa AAAA' WHERE id = 1", 1);
        assertQuery("SELECT id, full_name FROM " + tableName, "VALUES (1, 'aaaa AAAA'), (2, 'bbbb'), (3, 'cccc')");

        // update single row with compound predicate
        assertUpdate("UPDATE " + tableName + " SET full_name = 'aaaa' WHERE id = 1 and full_name='aaaa AAAA'", 1);
        assertQuery("SELECT id, full_name FROM " + tableName, "VALUES (1, 'aaaa'), (2, 'bbbb'), (3, 'cccc')");

        // update multiple rows at once
        assertUpdate("UPDATE " + tableName + " SET full_name = 'ssss' WHERE id != 1 ", 2);
        assertQuery("SELECT id, full_name FROM " + tableName, "VALUES (1, 'aaaa'), (2, 'ssss'), (3, 'ssss')");

        // update with filter matching no rows
        assertUpdate("UPDATE " + tableName + " SET full_name = 'ssss' WHERE id > 4 ", 0);
        assertQuery("SELECT id, full_name FROM " + tableName, "VALUES (1, 'aaaa'), (2, 'ssss'), (3, 'ssss')");

        // add column and update null values
        assertUpdate("ALTER TABLE  " + tableName + " ADD  column email varchar");
        assertQuery("SELECT id, full_name, email FROM " + tableName, "VALUES (1, 'aaaa', NULL), (2, 'ssss', NULL), (3, 'ssss', NULL)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'dddd', 'ddd@gmail.com')", 1);
        assertUpdate("UPDATE " + tableName + " SET email = 'abc@gmail.com' WHERE id in(1, 2, 3)", 3);
        assertQuery("SELECT id, full_name, email FROM " + tableName, "VALUES (1, 'aaaa', 'abc@gmail.com'), (2, 'ssss', 'abc@gmail.com'), (3, 'ssss', 'abc@gmail.com'), (4, 'dddd', 'ddd@gmail.com') ");

        // set all values to null
        assertUpdate("UPDATE " + tableName + " SET email = NULL", 4);
        assertQuery("SELECT email FROM " + tableName + " WHERE email is NULL", "VALUES NULL, NULL, NULL, NULL");

        // update nulls to non-null
        assertUpdate("UPDATE " + tableName + " SET email = 'test@gmail.com' WHERE email is NULL", 4);
        assertQuery("SELECT count(*) FROM " + tableName + " WHERE email is not NULL", "VALUES 4");
    }

    @Test
    public void testUpdateAllValues()
    {
        String tableName = "test_update_all_values_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(a int, b int, c int)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3), (11, 12, 13), (21, 22, 23)", 3);
        assertUpdate("UPDATE " + tableName + " SET a = a + 1, b = b - 1, c = c * 2", 3);
        assertQuery("SELECT * FROM " + tableName, "VALUES (2, 1, 6), (12, 11, 26), (22, 21, 46)");

        // update multiple columns with predicate
        assertUpdate("UPDATE " + tableName + " SET a = a + 1, b = b - 1, c = c * 2 WHERE a = 2 AND b = 1", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (3, 0, 12), (12, 11, 26), (22, 21, 46)");
    }

    @Test
    public void testUpdateRowType()
    {
        String tableName = "test_update_row_type" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(int_t INT, row_t ROW(f1 INT, f2 INT))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(2, 3)), (11, ROW(12, 13)), (21, ROW(22, 23))", 3);
        assertUpdate("UPDATE " + tableName + " SET int_t = int_t - 1 WHERE row_t.f2 = 3", 1);
        assertQuery("SELECT int_t, row_t.f1, row_t.f2 FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 13), (21, 22, 23)");
        assertUpdate("UPDATE " + tableName + " SET row_t = ROW(row_t.f1, row_t.f2 + 1) WHERE int_t = 11", 1);
        assertQuery("SELECT int_t, row_t.f1, row_t.f2 FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 22, 23)");
        assertUpdate("UPDATE " + tableName + " SET row_t = ROW(row_t.f1 * 2, row_t.f2) WHERE row_t.f1 = 22", 1);
        assertQuery("SELECT int_t, row_t.f1, row_t.f2 FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 44, 23)");
    }

    public void testUpdateOnPartitionTable()
    {
        String tableName = "test_update_partition_column_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(a int, b varchar(10))" + "with(partitioning=ARRAY['a'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'first'), (1, 'second'), (2, 'third')", 3);
        // update all rows on partition column
        assertUpdate("UPDATE " + tableName + " SET a = a + 1", 3);
        assertQuery("SELECT a, b FROM " + tableName, "VALUES (2,'first'), (2,'second'), (3,'third')");

        assertUpdate("UPDATE " + tableName + " SET a = a + (CASE b WHEN 'first' THEN 1 ELSE 0 END)", 3);
        assertQuery("SELECT a, b FROM " + tableName, "VALUES (3,'first'), (2,'second'), (3,'third')");

        // update on partition column with predicate
        assertUpdate("UPDATE " + tableName + " SET a = a + 1 WHERE b = 'second'", 1);
        assertQuery("SELECT a, b FROM " + tableName, "VALUES (3,'first'), (3,'second'), (3,'third')");

        // update non-partition column on a partitioned table without a predicate
        assertUpdate("UPDATE " + tableName + " SET a = a + 1 WHERE b = 'second'", 1);
        assertQuery("SELECT a, b FROM " + tableName, "VALUES (3,'first'), (4,'second'), (3,'third')");

        // update non-partition column on a partitioned table with a predicate
        assertUpdate("UPDATE " + tableName + " SET b = CONCAT(CAST(a as varchar), CASE a WHEN 1 THEN 'st' WHEN 2 THEN 'nd' WHEN 3 THEN 'rd' ELSE 'th' END) WHERE b = 'second'", 1);
        assertQuery("SELECT a, b FROM " + tableName, "VALUES (3,'first'), (4,'4th'), (3,'third')");
    }

    @DataProvider
    public Object[][] partitionedProvider()
    {
        return new Object[][] {
                {""}, // Without partitions.
                {"WITH (partitioning = ARRAY['address'])"}
        };
    }

    @Test(dataProvider = "partitionedProvider")
    public void testMergeSimpleQuery(String partitioning)
    {
        String targetTable = "merge_query_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) %s", targetTable, partitioning));
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING ", targetTable) +
                        "(VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')) AS s(customer, purchases, address) " +
                        "ON (t.customer = s.customer) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET purchases = s.purchases + t.purchases, address = s.address " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sqlMergeCommand, 4);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSimpleQueryPartitioned()
    {
        String targetTable = "merge_simple_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['customer'])", targetTable));
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING ", targetTable) +
                        "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address) " +
                        "ON (t.customer = s.customer) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET purchases = s.purchases + t.purchases, address = s.address " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sqlMergeCommand, 4);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithoutTablesAliases()
    {
        String targetTable = "test_without_aliases_target_" + randomTableSuffix();
        String sourceTable = "test_without_aliases_source_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable));
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s USING %s ", targetTable, sourceTable) +
                format("ON (%s.customer = %s.customer) ", targetTable, sourceTable) +
                format("WHEN MATCHED THEN" +
                        "    UPDATE SET purchases = %s.purchases + %s.purchases, address = %s.address ", sourceTable, targetTable, sourceTable) +
                format("WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases, address) VALUES(%s.customer, %s.purchases, %s.address)", sourceTable, sourceTable, sourceTable);

        assertUpdate(sqlMergeCommand, 4);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeUsingUpdateAndInsert()
    {
        String targetTable = "merge_simple_target_" + randomTableSuffix();
        String sourceTable = "merge_simple_source_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable));
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING %s s ", targetTable, sourceTable) +
                        "ON (t.customer = s.customer) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET purchases = s.purchases + t.purchases, address = s.address " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sqlMergeCommand, 4);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeOnlyInsertNewRows()
    {
        String targetTable = "merge_inserts_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable));
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable), 2);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING ", targetTable) +
                        "(VALUES ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')) AS s(customer, purchases, address)" +
                        "ON (t.customer = s.customer)" +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sqlMergeCommand, 2);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeOnlyUpdateExistingRows()
    {
        String targetTable = "merge_all_columns_updated_target_" + randomTableSuffix();
        String sourceTable = "merge_all_columns_updated_source_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable));
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Devon'), ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge')", targetTable), 4);
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire'), ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Ed', 7, 'Etherville')", sourceTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING %s s ", targetTable, sourceTable) +
                        "ON (t.customer = s.customer) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET customer = CONCAT(t.customer, '_updated'), purchases = s.purchases + t.purchases, address = s.address";

        assertUpdate(sqlMergeCommand, 3);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Dave_updated', 22, 'Darbyshire'), ('Aaron_updated', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol_updated', 12, 'Centreville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] partitionedAndBucketedProvider()
    {
        return new Object[][] {
                {""}, // Without partitions.
                {"WITH (partitioning = ARRAY['customer'])"},
                {"WITH (partitioning = ARRAY['purchases'])"},
                {"WITH (partitioning = ARRAY['bucket(customer, 3)'])"},
                {"WITH (partitioning = ARRAY['bucket(purchases, 4)'])"},
        };
    }

    @Test(dataProvider = "partitionedAndBucketedProvider")
    public void testMergeUsingSelectQuery(String partitioning)
    {
        String targetTable = "merge_various_target_" + randomTableSuffix();
        String sourceTable = "merge_various_source_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases VARCHAR) %s", targetTable, partitioning));
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);
        assertUpdate(format("INSERT INTO %s (customer, purchases) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING (SELECT customer, purchases FROM %s) s ", targetTable, sourceTable) +
                        "ON (t.purchases = s.purchases) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET customer = CONCAT(t.customer, '_', s.customer) " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases) VALUES(s.customer, s.purchases)";

        assertUpdate(sqlMergeCommand, 3);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Dave', 'dates'), ('Carol_Craig', 'candles'), ('Lou_Len', 'limes'), ('Joe', 'jellybeans')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test(dataProvider = "partitionedAndBucketedProvider")
    public void testMultipleMergeCommands(String partitioning)
    {
        int targetCustomerCount = 32;
        String targetTable = "merge_multiple_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR) %s", targetTable, partitioning));

        // joe_1, 1000, 91000, jan_1, 1 Poe Ct
        // ...
        // joe_15, 1000, 91000, jan_15, 15 Poe Ct
        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(Collectors.joining(", "));

        // joe_16, 2000, 92000, jan_16, 16 Poe Ct
        // ...
        // joe_32, 2000, 92000, jan_32, 32 Poe Ct
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) " +
                "VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount - 1);

        // joe_16, 3000, 83000, jan_16, 16 Eop Ct
        // ...
        // joe_32, 3000, 83000, jan_32, 32 Eop Ct
        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(Collectors.joining(", "));

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                        "ON t.customer = s.customer " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address";

        assertUpdate(sqlMergeCommand, targetCustomerCount / 2);

        assertQuery(
                format("SELECT customer, purchases, zipcode, spouse, address FROM %s", targetTable),
                format("VALUES %s, %s", originalInsertFirstHalf, firstMergeSource));

        // jack_32, 4000, 74000, jan_32, 32 Poe Ct
        // ...
        // jack_48, 4000, 74000, jan_48, 48 Poe Ct
        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

        // joe_1, 5000, 85000, jen_32, 32 Poe Ct
        // ...
        // joe_48, 5000, 85000, jen_48, 48 Poe Ct
        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        // Note that the following MERGE INTO does not update the "purchases" column.
        sqlMergeCommand =
                format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
                        "ON t.customer = s.customer " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases, zipcode, spouse, address) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address)";

        assertUpdate(sqlMergeCommand, targetCustomerCount * 3 / 2 - 1);

        // joe_1, 1000, 85000, jen_1, 1 Poe Ct
        // ...
        // joe_15, 1000, 85000, jen_15, 15 Poe Ct
        String updatedFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 1000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        // joe_16, 3000, 85000, jen_16, 16 Poe Ct
        // ...
        // joe_32, 3000, 85000, jen_32, 32 Poe Ct
        String updatedSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 3000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        // jack_32, 4000, 74000, jan_32, 32 Poe Ct
        // ...
        // jack_48, 4000, 74000, jan_48, 48 Poe Ct
        String nonUpdatedRows = nextInsert;

        // joe_32, 5000, 85000, jen_32, 32 Poe Ct
        // ...
        // joe_48, 5000, 85000, jen_48, 48 Poe Ct
        String insertedRows = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertQuery(
                format("SELECT customer, purchases, zipcode, spouse, address FROM %s", targetTable),
                format("VALUES %s, %s, %s, %s", updatedFirstHalf, updatedSecondHalf, nonUpdatedRows, insertedRows));

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeMillionRows()
    {
        String tableName = "test_merge_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (orderkey BIGINT, custkey BIGINT, totalprice DOUBLE)", tableName));

        // Initialize the merge target table with data:
        // When "mod(orderkey, 3) = 0" -> copy rows, when "mod(orderkey, 3) = 1" -> double price, when "mod(orderkey, 3) = 2" ->  rows with new orderkey
        assertUpdate(
                format("INSERT INTO %s " +
                                "SELECT orderkey, custkey, totalprice FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 0 " + // rows copied
                                "UNION ALL " +
                                "SELECT orderkey, custkey, 2*totalprice as totalprice FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 1 " + // rows with updated price
                                "UNION ALL " +
                                "SELECT orderkey + 100000002 as orderkey, custkey, totalprice as totalprice FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 2", // rows with new orderkey
                        tableName),
                (long) computeActual("SELECT count(*) FROM tpch.sf1.orders").getOnlyValue());

        // verify copied rows: same total price
        assertEquals(
                computeActual("SELECT count(*), round(sum(totalprice)) FROM " + tableName + " WHERE mod(orderkey, 3) = 0"),
                computeActual("SELECT count(*), round(sum(totalprice)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 0"));

        // verify rows will be updated: double total price
        assertEquals(
                computeActual("SELECT count(*), round(sum(totalprice)) FROM " + tableName + " WHERE mod(orderkey, 3) = 1"),
                computeActual("SELECT count(*), round(2*sum(totalprice)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 1"));

        // verify rows will be inserted: same total price and different orderkey.
        assertEquals(
                computeActual("SELECT count(*), round(sum(totalprice)) FROM " + tableName + " WHERE mod(orderkey, 3) = 2"),
                computeActual("SELECT count(*), round(sum(totalprice)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 2"));

        // MERGE INTO command to update the price of the existing orders and insert new orders, multiplying the original price by 3.
        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING (SELECT * FROM tpch.sf1.orders) s ", tableName) +
                        "ON (t.orderkey = s.orderkey) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET totalprice = s.totalprice " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (orderkey, custkey, totalprice) VALUES (s.orderkey, s.custkey, 3*s.totalprice)";

        assertUpdate(sqlMergeCommand, 1_500_000);

        // verify unmodified rows: same total price
        assertEquals(
                computeActual("SELECT count(*), round(sum(totalprice)) FROM " + tableName + " WHERE mod(orderkey, 3) = 0"),
                computeActual("SELECT count(*), round(sum(totalprice)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 0"));
        assertEquals(
                computeActual("SELECT count(*), round(sum(totalprice)) FROM " + tableName + " WHERE mod(orderkey, 3) = 2 AND orderkey > 100000002"),
                computeActual("SELECT count(*), round(sum(totalprice)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 2"));

        // verify updated rows: same total price (these rows originally had double total price in the target table)
        assertEquals(
                computeActual("SELECT count(*), round(sum(totalprice)) FROM " + tableName + " WHERE mod(orderkey, 3) = 1"),
                computeActual("SELECT count(*), round(sum(totalprice)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 1"));

        // verify inserted rows: triple original price
        assertEquals(
                computeActual("SELECT count(*), round(sum(totalprice)) FROM " + tableName + " WHERE mod(orderkey, 3) = 2 AND orderkey < 100000002"),
                computeActual("SELECT count(*), round(3*sum(totalprice)) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 2"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeQueryWithWeirdColumnsCapitalization()
    {
        String targetTable = "merge_weird_capitalization_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable));
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING ", targetTable.toUpperCase(ENGLISH)) +
                        "(VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')) AS s(customer, purchases, address) " +
                        "ON (t.customer = s.customer) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET purCHases = s.PurchaseS + t.pUrchases, aDDress = s.addrESs " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (CUSTOMER, purchases, addRESS) VALUES(s.custoMer, s.Purchases, s.ADDress)";

        assertUpdate(sqlMergeCommand, 4);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithMultipleConditions()
    {
        String targetTable = "merge_predicates_target_" + randomTableSuffix();
        String sourceTable = "merge_predicates_source_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", targetTable));
        assertUpdate(format("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Dave', 10, 'Devon'), (2, 'Dave', 20, 'Darbyshire')", targetTable), 2);
        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (3, 'Dave', 2, 'Madrid'), (4, 'Dave', 15, 'Barcelona')", sourceTable), 2);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING %s s ", targetTable, sourceTable) +
                        "ON t.customer = s.customer AND s.purchases < 6 " +
                        "WHEN MATCHED " +
                        "    THEN UPDATE SET purchases = s.purchases + t.purchases, address = concat(t.address, '/', s.address) " +
                        "WHEN NOT MATCHED" +
                        "    THEN INSERT (id, customer, purchases, address) VALUES (s.id, s.customer, s.purchases, s.address)";

        assertUpdate(sqlMergeCommand, 3);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES (1, 'Dave', 12, 'Devon/Madrid'), (2, 'Dave', 22, 'Darbyshire/Madrid'), (4, 'Dave', 15, 'Barcelona')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeCasts()
    {
        String targetTable = "merge_cast_target_" + randomTableSuffix();
        String sourceTable = "merge_cast_source_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (col1 INT, col2 BIGINT, col3 REAL, col4 DOUBLE, col5 DOUBLE)", targetTable));
        assertUpdate(format("CREATE TABLE %s (col1 INT, col2 INT, col3 INT, col4 INT, col5 REAL)", sourceTable));

        assertUpdate(format("INSERT INTO %s VALUES (1, 2, 3, 4, 5)", targetTable), 1);
        assertUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6)", sourceTable), 1);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING %s s ", targetTable, sourceTable) +
                        "ON (t.col1 + 1 = s.col1) " + // Note that the merge condition contains a sum.
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET col1 = s.col1, col2 = s.col2, col3 = s.col3, col4 = s.col4, col5 = s.col5";

        assertUpdate(sqlMergeCommand, 1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (2, 3, 4.0, 5.0, 6.0)");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSubqueries()
    {
        String targetTable = "merge_nation_target_" + randomTableSuffix();
        String sourceTable = "merge_nation_source_" + randomTableSuffix();

        assertUpdate(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", targetTable));
        assertUpdate(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (nation_name, region_name) VALUES ('GERMANY', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('FRANCE', 'EUROPE')", targetTable), 3);
        assertUpdate(format("INSERT INTO %s VALUES ('ALGERIA', 'AFRICA'), ('FRANCE', 'EUROPE'), ('EGYPT', 'MIDDLE EAST'), ('RUSSIA', 'EUROPE')", sourceTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING %s s ", targetTable, sourceTable) +
                        "ON (t.nation_name = s.nation_name) " +
                        "WHEN MATCHED " +
                        "    THEN UPDATE SET region_name = (SELECT CONCAT(name, '_UPDATED') FROM tpch.tiny.region WHERE name = t.region_name) " +
                        "WHEN NOT MATCHED " +
                        "    THEN INSERT VALUES(s.nation_name, (SELECT CONCAT(name, '_INSERTED') FROM tpch.tiny.region WHERE name = s.region_name))";

        assertUpdate(sqlMergeCommand, 4);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('GERMANY', 'EUROPE'), " +
                        "('ALGERIA', 'AFRICA_UPDATED'), ('FRANCE', 'EUROPE_UPDATED'), " +
                        "('EGYPT', 'MIDDLE EAST_INSERTED'), ('RUSSIA', 'EUROPE_INSERTED')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] partitionedBucketedFailure()
    {
        return new Object[][] {
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)"},
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['customer'])"},
                {"CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (partitioning = ARRAY['address'])"},
                {"CREATE TABLE %s (purchases INT, customer VARCHAR, address VARCHAR) WITH (partitioning = ARRAY['customer', 'address'])"},
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (partitioning = ARRAY['bucket(customer, 3)'])"}
        };
    }

    @Test(dataProvider = "partitionedBucketedFailure")
    public void testMergeMultipleRowsMatchMustFails(String createTableSql)
    {
        String targetTable = "merge_multiple_rows_match_target_" + randomTableSuffix();
        String sourceTable = "merge_multiple_rows_match_source_" + randomTableSuffix();

        assertUpdate(format(createTableSql, targetTable));
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable), 2);
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Adelphi'), ('Aaron', 8, 'Ashland')", sourceTable), 2);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING %s s ", targetTable, sourceTable) +
                        "ON (t.customer = s.customer) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET address = s.address";

        assertQueryFails(sqlMergeCommand, ".*The MERGE INTO command requires each target row to match at most one source row.*");

        assertUpdate(format("DELETE FROM %s WHERE purchases = 8", sourceTable), 1);

        assertUpdate(sqlMergeCommand, 1);

        assertQuery("SELECT customer, purchases, address FROM " + targetTable,
                "VALUES ('Aaron', 5, 'Adelphi'), ('Bill', 7, 'Antioch')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] targetAndSourceWithDifferentPartitioning()
    {
        return new Object[][] {
                {
                    "target_flat_source_flat",
                    "",
                    ""
                },
                {
                    "target_partitioned_source_flat",
                    "WITH (partitioning = ARRAY['customer'])",
                    ""
                },
                {
                    "target_bucketed_source_flat",
                    "WITH (partitioning = ARRAY['bucket(customer, 3)'])",
                    ""
                },
                {
                    "target_partitioned_and_bucketed_source_flat",
                    "WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])",
                    ""
                },
                {
                    "target_partitioned_and_bucketed_source_partitioned",
                    "WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])",
                    "WITH (partitioning = ARRAY['customer'])"
                },
                {
                    "target_and_source_partitioned_and_bucketed",
                    "WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])",
                    "WITH (partitioning = ARRAY['address', 'bucket(customer, 3)'])"
                }
        };
    }

    @Test(dataProvider = "targetAndSourceWithDifferentPartitioning")
    public void testMergeWithDifferentPartitioning(String testDescription, String targetTablePartitioning, String sourceTablePartitioning)
    {
        String targetTable = format("%s_target_%s", testDescription, randomTableSuffix());
        String sourceTable = format("%s_source_%s", testDescription, randomTableSuffix());

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) %s", targetTable, targetTablePartitioning));
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) %s", sourceTable, sourceTablePartitioning));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);
        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        @Language("SQL") String sqlMergeCommand =
                format("MERGE INTO %s t USING %s s ", targetTable, sourceTable) +
                        "ON (t.customer = s.customer) " +
                        "WHEN MATCHED THEN" +
                        "    UPDATE SET purchases = s.purchases + t.purchases, address = s.address " +
                        "WHEN NOT MATCHED THEN" +
                        "    INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sqlMergeCommand, 4);

        assertQuery("SELECT * FROM " + targetTable,
                "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
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
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        File metastoreDir = getIcebergDataDirectoryPath(dataDirectory, catalogType.name(), new IcebergConfig().getFileFormat(), false).toFile();
        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + randomUUID();
        FileSystem fs = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), metadataDir);
        Path path = new Path(metadataDir, deleteFileName);
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
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        File metastoreDir = getIcebergDataDirectoryPath(dataDirectory, catalogType.name(), new IcebergConfig().getFileFormat(), false).toFile();
        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + randomUUID();
        FileSystem fs = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), metadataDir);
        Schema deleteRowSchema = icebergTable.schema().select(overwriteValues.keySet());
        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(HadoopOutputFile.fromPath(new Path(metadataDir, deleteFileName), fs))
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

    protected HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HiveS3Config hiveS3Config = new HiveS3Config();
        return getHdfsEnvironment(hiveClientConfig, metastoreClientConfig, hiveS3Config);
    }

    public static HdfsEnvironment getHdfsEnvironment(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig, HiveS3Config hiveS3Config)
    {
        S3ConfigurationUpdater s3ConfigurationUpdater = new PrestoS3ConfigurationUpdater(hiveS3Config);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig, s3ConfigurationUpdater, ignored -> {}),
                ImmutableSet.of(), hiveClientConfig);
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
        tableName = normalizeIdentifier(tableName, ICEBERG_CATALOG);
        Catalog catalog = CatalogUtil.loadCatalog(catalogType.getCatalogImpl(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of("tpch", tableName));
    }

    protected Map<String, String> getProperties()
    {
        Path metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    protected Path getCatalogDirectory()
    {
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        switch (catalogType) {
            case HIVE:
            case HADOOP:
            case NESSIE:
                return new Path(getIcebergDataDirectoryPath(dataDirectory, catalogType.name(), new IcebergConfig().getFileFormat(), false).toFile().toURI());
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

    @Test
    public void testSortByAllTypes()
    {
        String tableName = "test_sort_by_all_types_" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  a_boolean boolean, " +
                "  an_integer integer, " +
                "  a_bigint bigint, " +
                "  a_real real, " +
                "  a_double double, " +
                "  a_short_decimal decimal(5,2), " +
                "  a_long_decimal decimal(38,20), " +
                "  a_varchar varchar, " +
                "  a_varbinary varbinary, " +
                "  a_date date, " +
                "  a_timestamp timestamp, " +
                "  an_array array(varchar), " +
                "  a_map map(integer, varchar) " +
                ") " +
                "WITH (" +
                "sorted_by = ARRAY[" +
                "  'a_boolean', " +
                "  'an_integer', " +
                "  'a_bigint', " +
                "  'a_real', " +
                "  'a_double', " +
                "  'a_short_decimal', " +
                "  'a_long_decimal', " +
                "  'a_varchar', " +
                "  'a_varbinary', " +
                "  'a_date', " +
                "  'a_timestamp' " +
                "  ]" +
                ")");
        String values = "(" +
                "true, " +
                "1, " +
                "BIGINT '2', " +
                "REAL '3.0', " +
                "DOUBLE '4.0', " +
                "DECIMAL '5.00', " +
                "CAST(DECIMAL '6.00' AS decimal(38,20)), " +
                "VARCHAR 'seven', " +
                "X'88888888', " +
                "DATE '2022-09-09', " +
                "TIMESTAMP '2022-11-11 11:11:11.000000', " +
                "ARRAY[VARCHAR 'four', 'teen'], " +
                "MAP(ARRAY[15], ARRAY[VARCHAR 'fifteen']))";
        String highValues = "(" +
                "true, " +
                "999999999, " +
                "BIGINT '999999999', " +
                "REAL '999.999', " +
                "DOUBLE '999.999', " +
                "DECIMAL '999.99', " +
                "DECIMAL '6.00', " +
                "'zzzzzzzzzzzzzz', " +
                "X'FFFFFFFF', " +
                "DATE '2099-12-31', " +
                "TIMESTAMP '2099-12-31 23:59:59.000000', " +
                "ARRAY['zzzz', 'zzzz'], " +
                "MAP(ARRAY[999], ARRAY['zzzz']))";
        String lowValues = "(" +
                "false, " +
                "0, " +
                "BIGINT '0', " +
                "REAL '0', " +
                "DOUBLE '0', " +
                "DECIMAL '0', " +
                "DECIMAL '0', " +
                "'', " +
                "X'00000000', " +
                "DATE '2000-01-01', " +
                "TIMESTAMP '2000-01-01 00:00:00.000000', " +
                "ARRAY['', ''], " +
                "MAP(ARRAY[0], ARRAY['']))";

        assertUpdate("INSERT INTO " + tableName + " VALUES " + values + ", " + highValues + ", " + lowValues, 3);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testEmptySortedByList()
    {
        String tableName = "test_empty_sorted_by_list_" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (a_boolean boolean, an_integer integer) " +
                "  WITH (partitioning = ARRAY['an_integer'], sorted_by = ARRAY[])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (false, 3), (true, 1), (true, 5), (false, 6), (false, 2)", 5);
        dropTable(getSession(), tableName);
    }

    @Test(dataProvider = "sortedTableWithQuotedIdentifierCasing")
    public void testCreateSortedTableWithQuotedIdentifierCasing(String columnName, String sortField)
    {
        String tableName = "test_create_sorted_table_with_quotes_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (%s bigint) WITH (sorted_by = ARRAY['%s'])", tableName, columnName, sortField));
        assertUpdate(format("INSERT INTO " + tableName + " VALUES (1),(3),(2),(5),(4)"), 5);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testSortedByAscNullFirstOrder()
    {
        String tableName = "test_empty_sorted_by_asc_null_first_" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (id int, name varchar(20))" +
                "  WITH ( sorted_by = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'ddd'), (20, 'ccc'), (null, 'vvv'), (3, 'aaa'), (5, 'eeee')", 5);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testSortedByAscNullLastOrder()
    {
        String tableName = "test_empty_sorted_by_asc_null_last_" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (id int, name varchar(20))" +
                "  WITH ( sorted_by = ARRAY['id ASC NULLS LAST'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'ddd'), (20, 'ccc'), (null, 'vvv'), (3, 'aaa'), (5, 'eeee')", 5);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testSortedByDescNullFirstOrder()
    {
        String tableName = "test_empty_sorted_by_desc_null_first_" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (id int, name varchar(20))" +
                "  WITH ( sorted_by = ARRAY['id DESC NULLS FIRST'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'ddd'), (20, 'ccc'), (null, 'vvv'), (3, 'aaa'), (5, 'eeee')", 5);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testSortedByDescNullLastOrder()
    {
        String tableName = "test_empty_sorted_by_desc_null_last" + randomTableSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (id int, name varchar(20))" +
                "  WITH ( sorted_by = ARRAY['id DESC NULLS LAST'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'ddd'), (20, 'ccc'), (null, 'vvv'), (3, 'aaa'), (5, 'eeee')", 5);
        dropTable(getSession(), tableName);
    }

    @DataProvider(name = "sortedTableWithQuotedIdentifierCasing")
    public static Object[][] sortedTableWithQuotedIdentifierCasing()
    {
        return new Object[][] {
                {"col", "col"},
                {"\"col\"", "col"},
                {"col", "\"col\""},
                {"\"col\"", "\"col\""},
        };
    }

    @Test(dataProvider = "sortedTableWithSortTransform")
    public void testCreateSortedTableWithSortTransform(String columnName, String sortField)
    {
        String tableName = "test_sort_with_transform_" + randomTableSuffix();
        String query = format("CREATE TABLE %s (%s TIMESTAMP) WITH (sorted_by = ARRAY['%s'])", tableName, columnName, sortField);
        assertQueryFails(query, Pattern.quote(format("Unable to parse sort field: [%s]", sortField)));
    }

    public void testStatisticsFileCacheInvalidationProcedure()
    {
        assertQuerySucceeds("CREATE TABLE test_statistics_file_cache_procedure(i int)");
        assertUpdate("INSERT INTO test_statistics_file_cache_procedure VALUES 1, 2, 3, 4, 5", 5);
        assertQuerySucceeds("ANALYZE test_statistics_file_cache_procedure");
        for (int i = 0; i < 3; i++) {
            assertQuerySucceeds("SHOW STATS FOR test_statistics_file_cache_procedure");
        }

        String jmxMetricsQuery = format("SELECT sum(\"cachestats.hitcount\"), sum(\"cachestats.size\"), sum(\"cachestats.misscount\") " +
                "from jmx.current.\"com.facebook.presto.iceberg.statistics:name=%s,type=statisticsfilecache\"", getSession().getCatalog().get());

        MaterializedResult result = computeActual(jmxMetricsQuery);
        long afterHitCount = (long) result.getMaterializedRows().get(0).getField(0);
        long afterCacheSize = (long) result.getMaterializedRows().get(0).getField(1);
        long afterMissCount = (long) result.getMaterializedRows().get(0).getField(2);
        assertTrue(afterHitCount > 0);
        assertTrue(afterCacheSize > 0);

        //test invalidate_statistics_file_cache procedure
        assertQuerySucceeds(format("CALL %s.system.invalidate_statistics_file_cache()", getSession().getCatalog().get()));
        MaterializedResult resultAfterProcedure = computeActual(jmxMetricsQuery);
        long afterProcedureCacheSize = (long) resultAfterProcedure.getMaterializedRows().get(0).getField(1);
        assertTrue(afterProcedureCacheSize == 0);

        assertQuerySucceeds("SHOW STATS FOR test_statistics_file_cache_procedure");

        MaterializedResult resultAfter = computeActual(jmxMetricsQuery);
        long newCacheSize = (long) resultAfter.getMaterializedRows().get(0).getField(1);
        long newMissCount = (long) resultAfter.getMaterializedRows().get(0).getField(2);
        assertTrue(newCacheSize > 0);
        assertTrue(afterMissCount < newMissCount);

        getQueryRunner().execute("DROP TABLE test_statistics_file_cache_procedure");
    }

    @DataProvider(name = "sortedTableWithSortTransform")
    public static Object[][] sortedTableWithSortTransform()
    {
        return new Object[][] {
                {"col", "bucket(col, 3)"},
                {"col", "bucket(\"col\", 3)"},
                {"col", "truncate(col, 3)"},
                {"col", "year(col)"},
                {"col", "month(col)"},
                {"col", "date(col)"},
                {"col", "hour(col)"},
        };
    }

    protected void dropTable(Session session, String table)
    {
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
    }

    @Test
    public void testEqualityDeleteAsJoinWithMaximumFieldsLimitUnderLimit()
            throws Exception
    {
        int maxColumns = 10;
        String tableName = "test_eq_delete_under_max_cols_" + randomTableSuffix();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, DELETE_AS_JOIN_REWRITE_ENABLED, "true")
                // Make sure the max columns is set to one more than the number of columns in the table
                .setCatalogSessionProperty(ICEBERG_CATALOG, DELETE_AS_JOIN_REWRITE_MAX_DELETE_COLUMNS, "" + (maxColumns + 1))
                .build();

        try {
            // Test with exactly max columns - should work fine
            // Create table with specified number of columns
            List<String> columnDefinitions = IntStream.range(0, maxColumns)
                    .mapToObj(i -> "col_" + i + " varchar")
                    .collect(Collectors.toList());
            columnDefinitions.add(0, "id bigint");

            String createTableSql = "CREATE TABLE " + tableName + " (" +
                    String.join(", ", columnDefinitions) + ")";
            assertUpdate(session, createTableSql);

            // Insert test rows
            for (int row = 1; row <= 3; row++) {
                final int currentRow = row;
                List<String> values = IntStream.range(0, maxColumns)
                        .mapToObj(i -> "'val_" + currentRow + "_" + i + "'")
                        .collect(Collectors.toList());
                values.add(0, String.valueOf(currentRow));

                String insertSql = "INSERT INTO " + tableName + " VALUES (" +
                        String.join(", ", values) + ")";
                assertUpdate(session, insertSql, 1);
            }

            // Verify all rows exist
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES (3)");

            // Update table to format version 2 and create equality delete files
            Table icebergTable = updateTable(tableName);

            // Create equality delete using ALL columns
            Map<String, Object> deleteRow = new HashMap<>();
            deleteRow.put("id", 2L);
            for (int i = 0; i < maxColumns; i++) {
                deleteRow.put("col_" + i, "val_2_" + i);
            }

            // Write equality delete with ALL columns
            writeEqualityDeleteToNationTable(icebergTable, deleteRow);

            // Query should work correctly regardless of optimization
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES (2)");
            assertQuery(session, "SELECT id FROM " + tableName + " ORDER BY id", "VALUES (1), (3)");

            // With <= max columns, query plan should use JOIN (optimization enabled)
            assertPlan(session, "SELECT * FROM " + tableName,
                    anyTree(
                        node(JoinNode.class,
                            anyTree(tableScan(tableName)),
                            anyTree(tableScan(tableName)))));
        }
        finally {
            dropTable(session, tableName);
        }
    }

    @Test
    public void testEqualityDeleteAsJoinWithMaximumFieldsLimitOverLimit()
            throws Exception
    {
        int maxColumns = 10;
        String tableName = "test_eq_delete_max_cols_" + randomTableSuffix();

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, DELETE_AS_JOIN_REWRITE_ENABLED, "true")
                .setCatalogSessionProperty(ICEBERG_CATALOG, DELETE_AS_JOIN_REWRITE_MAX_DELETE_COLUMNS, "" + maxColumns)
                .build();

        try {
            // Test with max columns - optimization should be disabled to prevent stack overflow
            // Create table with specified number of columns
            List<String> columnDefinitions = IntStream.range(0, maxColumns)
                    .mapToObj(i -> "col_" + i + " varchar")
                    .collect(Collectors.toList());
            columnDefinitions.add(0, "id bigint");

            String createTableSql = "CREATE TABLE " + tableName + " (" +
                    String.join(", ", columnDefinitions) + ")";
            assertUpdate(session, createTableSql);

            // Insert test rows
            for (int row = 1; row <= 3; row++) {
                final int currentRow = row;
                List<String> values = IntStream.range(0, maxColumns)
                        .mapToObj(i -> "'val_" + currentRow + "_" + i + "'")
                        .collect(Collectors.toList());
                values.add(0, String.valueOf(currentRow));

                String insertSql = "INSERT INTO " + tableName + " VALUES (" +
                        String.join(", ", values) + ")";
                assertUpdate(session, insertSql, 1);
            }

            // Verify all rows exist
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES (3)");

            // Update table to format version 2 and create equality delete files
            Table icebergTable = updateTable(tableName);

            // Create equality delete using ALL columns
            Map<String, Object> deleteRow = new HashMap<>();
            deleteRow.put("id", 2L);
            for (int i = 0; i < maxColumns; i++) {
                deleteRow.put("col_" + i, "val_2_" + i);
            }

            // Write equality delete with ALL columns
            writeEqualityDeleteToNationTable(icebergTable, deleteRow);

            // Query should work correctly regardless of optimization
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES (2)");
            assertQuery(session, "SELECT id FROM " + tableName + " ORDER BY id", "VALUES (1), (3)");

            // With > max columns, optimization is disabled - no JOIN in plan
            // Verify the query works but doesn't contain a join node
            assertQuery(session, "SELECT * FROM " + tableName + " WHERE id = 1",
                    "VALUES (" + Stream.concat(Stream.of("1"),
                            IntStream.range(0, maxColumns).mapToObj(i -> "'val_1_" + i + "'"))
                            .collect(Collectors.joining(", ")) + ")");

            // To verify no join is present, we can check that the plan only contains table scan
            assertPlan(session, "SELECT * FROM " + tableName,
                    anyTree(
                            anyNot(JoinNode.class,
                                    tableScan(tableName))));
        }
        finally {
            dropTable(session, tableName);
        }
    }
}
