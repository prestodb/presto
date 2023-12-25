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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.TEST_CATALOG_DIRECTORY;
import static com.facebook.presto.iceberg.IcebergQueryRunner.TEST_DATA_DIRECTORY;
import static com.facebook.presto.iceberg.IcebergSessionProperties.DELETE_AS_JOIN_REWRITE_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.STATISTIC_SNAPSHOT_RECORD_DIFFERENCE_WEIGHT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.testng.Assert.assertTrue;

public class IcebergDistributedTestBase
        extends AbstractTestDistributedQueries
{
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

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    public void testRenameTable()
    {
    }

    @Override
    public void testRenameColumn()
    {
    }

    @Override
    public void testDelete()
    {
        // Test delete all rows
        long totalCount = (long) getQueryRunner().execute("CREATE TABLE test_delete as select * from lineitem")
                .getOnlyValue();
        assertUpdate("DELETE FROM test_delete", totalCount);
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 0L);
        assertQuerySucceeds("DROP TABLE test_delete");

        // Test delete whole partitions identified by one partition column
        totalCount = (long) getQueryRunner().execute("CREATE TABLE test_partitioned_drop WITH (partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem")
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
        totalCount = (long) getQueryRunner().execute("CREATE TABLE test_partitioned_drop WITH (partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem")
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

        // Do not support delete with filters about non-identity partition column
        String errorMessage1 = "This connector only supports delete where one or more partitions are deleted entirely";
        assertUpdate("CREATE TABLE test_partitioned_drop WITH (partitioning = ARRAY['bucket(orderkey, 2)', 'linenumber', 'linestatus']) as select * from lineitem", totalCount);
        assertQueryFails("DELETE FROM test_partitioned_drop WHERE orderkey = 1", errorMessage1);
        assertQueryFails("DELETE FROM test_partitioned_drop WHERE partkey > 100", errorMessage1);
        assertQueryFails("DELETE FROM test_partitioned_drop WHERE linenumber = 1 and orderkey = 1", errorMessage1);

        // Do not allow delete data at specified snapshot
        String errorMessage2 = "This connector do not allow delete data at specified snapshot";
        List<Long> snapshots = getQueryRunner().execute("SELECT snapshot_id FROM \"test_partitioned_drop$snapshots\"").getOnlyColumnAsSet()
                .stream().map(Long.class::cast).collect(Collectors.toList());
        for (long snapshot : snapshots) {
            assertQueryFails("DELETE FROM \"test_partitioned_drop@" + snapshot + "\" WHERE linenumber = 1", errorMessage2);
        }

        assertQuerySucceeds("DROP TABLE test_partitioned_drop");
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
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
        assertQuerySucceeds("insert into test_delete values(1, '1001', 10001), (2, '1003', 10001), (3, '1004', 10003)");
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 5L);

        // Deletion fails, because column 'd' do not exists in older partition specs
        String errorMessage = "This connector only supports delete where one or more partitions are deleted entirely";
        assertQueryFails("delete from test_delete where d = 1001", errorMessage);
        assertQueryFails("delete from test_delete where d = 1001 and c = 2", errorMessage);

        // Deletion succeeds, because column 'c' exists in all partition specs
        assertUpdate("DELETE FROM test_delete WHERE c = 1", 3);
        assertEquals(getQueryRunner().execute("SELECT count(*) FROM test_delete").getOnlyValue(), 2L);

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

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Test
    public void testPartitionedByTimestampType()
    {
        // create iceberg table partitioned by column of TimestampType, and insert some data
        assertQuerySucceeds("create table test_partition_columns(a bigint, b timestamp) with (partitioning = ARRAY['b'])");
        assertQuerySucceeds("insert into test_partition_columns values(1, timestamp '1984-12-08 00:10:00'), (2, timestamp '2001-01-08 12:01:01')");

        // validate return data of TimestampType
        List<Object> timestampColumnDatas = getQueryRunner().execute("select b from test_partition_columns order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(timestampColumnDatas.size(), 2);
        assertEquals(timestampColumnDatas.get(0), LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        assertEquals(timestampColumnDatas.get(1), LocalDateTime.parse("2001-01-08 12:01:01", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        // validate column of TimestampType exists in query filter
        assertEquals(getQueryRunner().execute("select b from test_partition_columns where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(),
                LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        assertEquals(getQueryRunner().execute("select b from test_partition_columns where b = timestamp '2001-01-08 12:01:01'").getOnlyValue(),
                LocalDateTime.parse("2001-01-08 12:01:01", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        // validate column of TimestampType in system table "partitions"
        assertEquals(getQueryRunner().execute("select count(*) FROM \"test_partition_columns$partitions\"").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns$partitions\" where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns$partitions\" where b = timestamp '2001-01-08 12:01:01'").getOnlyValue(), 1L);

        // validate column of TimestampType exists in delete filter
        assertUpdate("delete from test_partition_columns WHERE b = timestamp '2001-01-08 12:01:01'", 1);
        timestampColumnDatas = getQueryRunner().execute("select b from test_partition_columns order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(timestampColumnDatas.size(), 1);
        assertEquals(timestampColumnDatas.get(0), LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        assertEquals(getQueryRunner().execute("select b FROM test_partition_columns where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(),
                LocalDateTime.parse("1984-12-08 00:10:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        assertEquals(getQueryRunner().execute("select count(*) from \"test_partition_columns$partitions\"").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns$partitions\" where b = timestamp '1984-12-08 00:10:00'").getOnlyValue(), 1L);

        assertQuerySucceeds("drop table test_partition_columns");
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

    @Override
    public void testDescribeOutput()
    {
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
    }

    @Override
    @Test
    public void testStringFilters()
    {
        // Type not supported for Iceberg: CHAR(10). Only test VARCHAR(10).
        assertUpdate("CREATE TABLE test_varcharn_filter (shipmode VARCHAR(10))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_varcharn_filter"));
        assertTableColumnNames("test_varcharn_filter", "shipmode");
        assertUpdate("INSERT INTO test_varcharn_filter SELECT shipmode FROM lineitem", 60175);

        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR'", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR    '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR       '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR            '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'NONEXIST'", "VALUES (0)");
    }

    @Test
    public void testReadWriteNDVs()
    {
        assertUpdate("CREATE TABLE test_stat_ndv (col0 int)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_stat_ndv"));
        assertTableColumnNames("test_stat_ndv", "col0");

        // test that stats don't exist before analyze
        TableStatistics stats = getTableStats("test_stat_ndv");
        assertTrue(stats.getColumnStatistics().isEmpty());

        // test after simple insert we get a good estimate
        assertUpdate("INSERT INTO test_stat_ndv VALUES 1, 2, 3", 3);
        getQueryRunner().execute("ANALYZE test_stat_ndv");
        stats = getTableStats("test_stat_ndv");
        assertEquals(stats.getColumnStatistics().values().stream().findFirst().get().getDistinctValuesCount(), Estimate.of(3.0));

        // test after inserting the same values, we still get the same estimate
        assertUpdate("INSERT INTO test_stat_ndv VALUES 1, 2, 3", 3);
        stats = getTableStats("test_stat_ndv");
        assertEquals(stats.getColumnStatistics().values().stream().findFirst().get().getDistinctValuesCount(), Estimate.of(3.0));

        // test after ANALYZING with the new inserts that the NDV estimate is the same
        getQueryRunner().execute("ANALYZE test_stat_ndv");
        stats = getTableStats("test_stat_ndv");
        assertEquals(stats.getColumnStatistics().values().stream().findFirst().get().getDistinctValuesCount(), Estimate.of(3.0));

        // test after inserting a new value, but not analyzing, the estimate is the same.
        assertUpdate("INSERT INTO test_stat_ndv VALUES 4", 1);
        stats = getTableStats("test_stat_ndv");
        assertEquals(stats.getColumnStatistics().values().stream().findFirst().get().getDistinctValuesCount(), Estimate.of(3.0));

        // test that after analyzing, the updates stats show up.
        getQueryRunner().execute("ANALYZE test_stat_ndv");
        stats = getTableStats("test_stat_ndv");
        assertEquals(stats.getColumnStatistics().values().stream().findFirst().get().getDistinctValuesCount(), Estimate.of(4.0));

        // test adding a null value is successful, and analyze still runs successfully
        assertUpdate("INSERT INTO test_stat_ndv VALUES NULL", 1);
        assertQuerySucceeds("ANALYZE test_stat_ndv");
        stats = getTableStats("test_stat_ndv");
        assertEquals(stats.getColumnStatistics().values().stream().findFirst().get().getDistinctValuesCount(), Estimate.of(4.0));

        assertUpdate("DROP TABLE test_stat_ndv");
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
            System.out.printf("distinct @ %s count col0: %s%n", snaps.get(i), col0Stats.getDistinctValuesCount());
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
        Function<Integer, Estimate> ndvs = (x) -> columnStatsFor(getTableStats("test_stat_dist", Optional.of(snapshots.get(x)), weightedSession), "col0")
                .getDistinctValuesCount();
        assertEquals(ndvs.apply(0).getValue(), 1);
        assertEquals(ndvs.apply(1).getValue(), 1);
        assertEquals(ndvs.apply(2).getValue(), 1);
        assertEquals(ndvs.apply(3).getValue(), 6);
        assertEquals(ndvs.apply(4).getValue(), 6);
        assertEquals(ndvs.apply(5).getValue(), 6);
        assertUpdate("DROP TABLE test_stat_dist");
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
        return getTableStats(name, snapshot, getSession());
    }

    private TableStatistics getTableStats(String name, Optional<Long> snapshot, Session session)
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
                new ArrayList<>(resolver.getColumnHandles(handle).values()),
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
        File metastoreDir = getDistributedQueryRunner().getCoordinator().getDataDirectory().toFile();
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
        File metastoreDir = getDistributedQueryRunner().getCoordinator().getDataDirectory().toFile();
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

    protected static HdfsEnvironment getHdfsEnvironment()
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
                return dataDirectory
                        .resolve(TEST_DATA_DIRECTORY)
                        .getParent()
                        .resolve(TEST_CATALOG_DIRECTORY)
                        .toFile();
            case HADOOP:
            case NESSIE:
                return dataDirectory.toFile();
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported Presto Iceberg catalog type " + catalogType);
    }
}
