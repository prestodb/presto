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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;

public class IcebergDistributedTestBase
        extends AbstractTestDistributedQueries
{
    private final CatalogType catalogType;

    protected IcebergDistributedTestBase(CatalogType catalogType)
    {
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), catalogType);
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

        // validate currently unsupported partition transform
        assertQueryFails("alter table add_partition_column add column e timestamp with(partitioning = 'year')", "Currently unsupported partition transform: year");
        assertQueryFails("alter table add_partition_column add column e timestamp with(partitioning = 'month')", "Currently unsupported partition transform: month");
        assertQueryFails("alter table add_partition_column add column e timestamp with(partitioning = 'day')", "Currently unsupported partition transform: day");
        assertQueryFails("alter table add_partition_column add column e timestamp with(partitioning = 'hour')", "Currently unsupported partition transform: hour");

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
    public void testPartitionedByVarbinaryType()
    {
        // create iceberg table partitioned by column of VarbinaryType, and insert some data
        assertQuerySucceeds("drop table if exists test_partition_columns_varbinary");
        assertQuerySucceeds("create table test_partition_columns_varbinary(a bigint, b varbinary) with (partitioning = ARRAY['b'])");
        assertQuerySucceeds("insert into test_partition_columns_varbinary values(1, X'bcd1'), (2, X'e3bcd1')");

        // validate return data of VarbinaryType
        List<Object> varbinaryColumnDatas = getQueryRunner().execute("select b from test_partition_columns_varbinary order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(varbinaryColumnDatas.size(), 2);
        assertEquals(varbinaryColumnDatas.get(0), new byte[]{(byte) 0xbc, (byte) 0xd1});
        assertEquals(varbinaryColumnDatas.get(1), new byte[]{(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});

        // validate column of VarbinaryType exists in query filter
        assertEquals(getQueryRunner().execute("select b from test_partition_columns_varbinary where b = X'bcd1'").getOnlyValue(),
                new byte[]{(byte) 0xbc, (byte) 0xd1});
        assertEquals(getQueryRunner().execute("select b from test_partition_columns_varbinary where b = X'e3bcd1'").getOnlyValue(),
                new byte[]{(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});

        // validate column of VarbinaryType in system table "partitions"
        assertEquals(getQueryRunner().execute("select count(*) FROM \"test_partition_columns_varbinary$partitions\"").getOnlyValue(), 2L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_varbinary$partitions\" where b = X'bcd1'").getOnlyValue(), 1L);
        assertEquals(getQueryRunner().execute("select row_count from \"test_partition_columns_varbinary$partitions\" where b = X'e3bcd1'").getOnlyValue(), 1L);

        // validate column of VarbinaryType exists in delete filter
        assertUpdate("delete from test_partition_columns_varbinary WHERE b = X'bcd1'", 1);
        varbinaryColumnDatas = getQueryRunner().execute("select b from test_partition_columns_varbinary order by a asc").getOnlyColumn().collect(Collectors.toList());
        assertEquals(varbinaryColumnDatas.size(), 1);
        assertEquals(varbinaryColumnDatas.get(0), new byte[]{(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});
        assertEquals(getQueryRunner().execute("select b FROM test_partition_columns_varbinary where b = X'e3bcd1'").getOnlyValue(),
                new byte[]{(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});
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

    private void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = (String) computeActual(query).getOnlyValue();

        assertTrue(value.matches("(?s:.*)CPU:.*, Input:.*, Output(?s:.*)"), format("Expected output to contain \"CPU:.*, Input:.*, Output\", but it is %s", value));
    }
}
