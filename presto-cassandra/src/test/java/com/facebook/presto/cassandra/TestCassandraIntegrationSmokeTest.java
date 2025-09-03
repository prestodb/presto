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
package com.facebook.presto.cassandra;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import static com.datastax.driver.core.utils.Bytes.toRawHexString;
import static com.facebook.presto.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static com.facebook.presto.cassandra.CassandraQueryRunner.createCassandraSession;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES_INSERT;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES_PARTITION_KEY;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS_INEQUALITY;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS_LARGE;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_MULTI_PARTITION_CLUSTERING_KEYS;
import static com.facebook.presto.cassandra.CassandraTestingUtils.createTestTables;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertContainsEventually;
import static com.google.common.primitives.Ints.toByteArray;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestCassandraIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final String KEYSPACE = "smoke_test";
    private static final Session SESSION = createCassandraSession(KEYSPACE);

    private static final Timestamp DATE_TIME_LOCAL = Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 3, 4, 5, 0));
    private static final LocalDateTime TIMESTAMP_LOCAL = LocalDateTime.of(1969, 12, 31, 23, 4, 5); // TODO #7122 should match DATE_TIME_LOCAL

    private CassandraServer server;
    private CassandraSession session;

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }

    @Override
    protected boolean isDateTypeSupported()
    {
        return false;
    }

    @Override
    protected boolean isParameterizedVarcharSupported()
    {
        return false;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CassandraServer server = new CassandraServer();
        this.server = server;
        this.session = server.getSession();
        createTestTables(session, server.getMetadata(), KEYSPACE, DATE_TIME_LOCAL);
        return createCassandraQueryRunner(server);
    }

    @Test
    public void testPartitionKeyPredicate()
    {
        String sql = "SELECT *" +
                " FROM " + TABLE_ALL_TYPES_PARTITION_KEY +
                " WHERE key = 'key 7'" +
                " AND typeuuid = '00000000-0000-0000-0000-000000000007'" +
                " AND typeinteger = 7" +
                " AND typelong = 1007" +
                " AND typebytes = from_hex('" + toRawHexString(ByteBuffer.wrap(toByteArray(7))) + "')" +
                " AND typetimestamp = TIMESTAMP '1969-12-31 23:04:05'" +
                " AND typeansi = 'ansi 7'" +
                " AND typeboolean = false" +
                " AND typedecimal = 128.0" +
                " AND typedouble = 16384.0" +
                " AND typefloat = REAL '2097152.0'" +
                " AND typeinet = '127.0.0.1'" +
                " AND typevarchar = 'varchar 7'" +
                " AND typevarint = '10000000'" +
                " AND typetimeuuid = 'd2177dd0-eaa2-11de-a572-001b779c76e7'" +
                " AND typelist = '[\"list-value-17\",\"list-value-27\"]'" +
                " AND typemap = '{7:8,9:10}'" +
                " AND typeset = '[false,true]'" +
                "";
        MaterializedResult result = execute(sql);

        assertEquals(result.getRowCount(), 1);
    }

    @Test
    public void testSelect()
    {
        assertSelect(TABLE_ALL_TYPES, false);
        assertSelect(TABLE_ALL_TYPES_PARTITION_KEY, false);
    }

    @Test
    public void testCreateTableAs()
    {
        execute("DROP TABLE IF EXISTS table_all_types_copy");
        execute("CREATE TABLE table_all_types_copy AS SELECT * FROM " + TABLE_ALL_TYPES);
        assertSelect("table_all_types_copy", true);
        execute("DROP TABLE table_all_types_copy");
    }

    @Test(enabled = false)
    public void testClusteringPredicates()
    {
        String sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key='key_1' AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key='key_1' AND clust_one!='clust_one'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2','key_3','key_4') AND clust_one='clust_one' AND clust_two>'clust_two_1'";
        assertEquals(execute(sql).getRowCount(), 3);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1') OR (clust_two='clust_two_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_three='clust_three_1'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')";
        assertEquals(execute(sql).getRowCount(), 2);
    }

    @Test
    public void testMultiplePartitionClusteringPredicates()
    {
        String partitionInPredicates = " partition_one IN ('partition_one_1','partition_one_2') AND partition_two IN ('partition_two_1','partition_two_2') ";
        String sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one!='clust_one'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " +
                "partition_one IN ('partition_one_1','partition_one_2','partition_one_3','partition_one_4') AND " +
                "partition_two IN ('partition_two_1','partition_two_2','partition_two_3','partition_two_4') AND " +
                "clust_one='clust_one' AND clust_two>'clust_two_1'";
        assertEquals(execute(sql).getRowCount(), 3);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1') OR (clust_two='clust_two_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND " +
                "((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND clust_three='clust_three_1'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_MULTI_PARTITION_CLUSTERING_KEYS + " WHERE " + partitionInPredicates + " AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')";
        assertEquals(execute(sql).getRowCount(), 2);
    }

    @Test (enabled = false)
    public void testClusteringKeyOnlyPushdown()
    {
        String sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 9);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE clust_one='clust_one' AND clust_two='clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'";
        assertEquals(execute(sql).getRowCount(), 1);

        // below test cases are needed to verify clustering key pushdown with unpartitioned table
        // for the smaller table (<200 partitions by default) connector fetches all the partitions id
        // and the partitioned patch is being followed
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two='clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two > 'clust_two_998'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two > 'clust_two_997' AND clust_two < 'clust_two_999'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_998'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three < 'clust_three_3'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_1' AND clust_three < 'clust_three_3'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two < 'clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_997','clust_two_998','clust_two_999') AND clust_two > 'clust_two_998'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_LARGE + " WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two = 'clust_two_2'";
        assertEquals(execute(sql).getRowCount(), 1);
    }

    @Test (enabled = false)
    public void testClusteringKeyPushdownInequality()
    {
        String sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one'";
        assertEquals(execute(sql).getRowCount(), 4);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1969-12-31 23:04:05.020'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1969-12-31 23:04:05.010'";
        assertEquals(execute(sql).getRowCount(), 0);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2)";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two > 1 AND clust_two < 3";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three >= timestamp '1969-12-31 23:04:05.010' AND clust_three <= timestamp '1969-12-31 23:04:05.020'";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2) AND clust_three >= timestamp '1969-12-31 23:04:05.010' AND clust_three <= timestamp '1969-12-31 23:04:05.020'";
        assertEquals(execute(sql).getRowCount(), 2);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two < 2";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two > 2";
        assertEquals(execute(sql).getRowCount(), 1);
        sql = "SELECT * FROM " + TABLE_CLUSTERING_KEYS_INEQUALITY + " WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two = 2";
        assertEquals(execute(sql).getRowCount(), 1);
    }

    @Test
    public void testUpperCaseNameUnescapedInCassandra()
    {
        /*
         * If an identifier is not escaped with double quotes it is stored as lowercase in the Cassandra metadata
         *
         * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/ucase-lcase_r.html
         */
        session.execute("CREATE KEYSPACE KEYSPACE_1 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_1")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE KEYSPACE_1.TABLE_1 (COLUMN_1 bigint PRIMARY KEY)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_1"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_1")
                .build(), new Duration(1, MINUTES));
        assertContains(execute("SHOW COLUMNS FROM cassandra.keyspace_1.table_1"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), BIGINT, BIGINT, BIGINT)
                .row("column_1", "bigint", "", "", 19L, null, null)
                .build());

        execute("INSERT INTO keyspace_1.table_1 (column_1) VALUES (1)");

        assertEquals(execute("SELECT column_1 FROM cassandra.keyspace_1.table_1").getRowCount(), 1);
        assertUpdate("DROP TABLE cassandra.keyspace_1.table_1");

        // when an identifier is unquoted the lowercase and uppercase spelling may be used interchangeable
        session.execute("DROP KEYSPACE keyspace_1");
    }

    @Test
    public void testUppercaseNameEscaped()
    {
        /*
         * If an identifier is escaped with double quotes it is stored verbatim
         *
         * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/ucase-lcase_r.html
         */
        session.execute("CREATE KEYSPACE \"KEYSPACE_2\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_2")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\" bigint PRIMARY KEY)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_2"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_2")
                .build(), new Duration(1, MINUTES));
        assertContains(execute("SHOW COLUMNS FROM cassandra.keyspace_2.table_2"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), BIGINT, BIGINT, BIGINT)
                .row("column_2", "bigint", "", "", 19L, null, null)
                .build());

        execute("INSERT INTO \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\") VALUES (1)");

        assertEquals(execute("SELECT column_2 FROM cassandra.keyspace_2.table_2").getRowCount(), 1);
        assertUpdate("DROP TABLE cassandra.keyspace_2.table_2");

        // when an identifier is unquoted the lowercase and uppercase spelling may be used interchangeable
        session.execute("DROP KEYSPACE \"KEYSPACE_2\"");
    }

    @Test
    public void testKeyspaceNameAmbiguity()
    {
        // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 keyspaces with names
        // that have differences only in letters case.
        session.execute("CREATE KEYSPACE \"KeYsPaCe_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE KEYSPACE \"kEySpAcE_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");

        // Although in Presto all the schema and table names are always displayed as lowercase
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_3")
                .row("keyspace_3")
                .build(), new Duration(1, MINUTES));

        // There is no way to figure out what the exactly keyspace we want to retrieve tables from
        assertQueryFailsEventually(
                "SHOW TABLES FROM cassandra.keyspace_3",
                "More than one keyspace has been found for the case insensitive schema name: keyspace_3 -> \\(KeYsPaCe_3, kEySpAcE_3\\)",
                new Duration(1, MINUTES));

        session.execute("DROP KEYSPACE \"KeYsPaCe_3\"");
        session.execute("DROP KEYSPACE \"kEySpAcE_3\"");
    }

    @Test
    public void testTableNameAmbiguity()
            throws Exception
    {
        session.execute("CREATE KEYSPACE keyspace_4 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_4")
                .build(), new Duration(1, MINUTES));

        // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 tables with names
        // that have differences only in letters case.
        session.execute("CREATE TABLE keyspace_4.\"TaBlE_4\" (column_4 bigint PRIMARY KEY)");
        session.execute("CREATE TABLE keyspace_4.\"tAbLe_4\" (column_4 bigint PRIMARY KEY)");

        // This is added for Cassandra to refresh its metadata so that we don't encounter a race condition in the forthcoming steps and achieve eventual consistency.
        Thread.sleep(1000);

        // Although in Presto all the schema and table names are always displayed as lowercase
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_4"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_4")
                .row("table_4")
                .build(), new Duration(1, MINUTES));

        // There is no way to figure out what the exactly table is being queried
        assertQueryFailsEventually(
                "SHOW COLUMNS FROM cassandra.keyspace_4.table_4",
                "More than one table has been found for the case insensitive table name: table_4 -> \\(TaBlE_4, tAbLe_4\\)",
                new Duration(1, MINUTES));
        assertQueryFailsEventually(
                "SELECT * FROM cassandra.keyspace_4.table_4",
                "More than one table has been found for the case insensitive table name: table_4 -> \\(TaBlE_4, tAbLe_4\\)",
                new Duration(1, MINUTES));
        session.execute("DROP KEYSPACE keyspace_4");
    }

    @Test
    public void testColumnNameAmbiguity()
    {
        session.execute("CREATE KEYSPACE keyspace_5 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_5")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE keyspace_5.table_5 (\"CoLuMn_5\" bigint PRIMARY KEY, \"cOlUmN_5\" bigint)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_5"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_5")
                .build(), new Duration(1, MINUTES));

        assertQueryFailsEventually(
                "SHOW COLUMNS FROM cassandra.keyspace_5.table_5",
                ".*More than one column has been found for the case insensitive column name: column_5 -> \\(CoLuMn_5, cOlUmN_5\\)",
                new Duration(1, MINUTES));
        assertQueryFailsEventually(
                "SELECT * FROM cassandra.keyspace_5.table_5",
                ".*More than one column has been found for the case insensitive column name: column_5 -> \\(CoLuMn_5, cOlUmN_5\\)",
                new Duration(1, MINUTES));

        session.execute("DROP KEYSPACE keyspace_5");
    }

    @Test
    public void testInsert()
    {
        String sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                " FROM " + TABLE_ALL_TYPES_INSERT;
        assertEquals(execute(sql).getRowCount(), 0);

        // TODO Following types are not supported now. We need to change null into the value after fixing it
        // blob, frozen<set<type>>, inet, list<type>, map<type,type>, set<type>, timeuuid, decimal, uuid, varint
        // timestamp can be inserted but the expected and actual values are not same
        execute("INSERT INTO " + TABLE_ALL_TYPES_INSERT + " (" +
                "key," +
                "typeuuid," +
                "typeinteger," +
                "typelong," +
                "typebytes," +
                "typetimestamp," +
                "typeansi," +
                "typeboolean," +
                "typedecimal," +
                "typedouble," +
                "typefloat," +
                "typeinet," +
                "typevarchar," +
                "typevarint," +
                "typetimeuuid," +
                "typelist," +
                "typemap," +
                "typeset" +
                ") VALUES (" +
                "'key1', " +
                "null, " +
                "1, " +
                "1000, " +
                "null, " +
                "timestamp '1970-01-01 08:34:05.0', " +
                "'ansi1', " +
                "true, " +
                "null, " +
                "0.3, " +
                "cast('0.4' as real), " +
                "null, " +
                "'varchar1', " +
                "null, " +
                "null, " +
                "null, " +
                "null, " +
                "null " +
                ")");

        MaterializedResult result = execute(sql);
        int rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key1",
                null,
                1,
                1000L,
                null,
                LocalDateTime.of(1970, 1, 1, 8, 34, 5),
                "ansi1",
                true,
                null,
                0.3,
                (float) 0.4,
                null,
                "varchar1",
                null,
                null,
                null,
                null,
                null));

        // insert null for all datatypes
        execute("INSERT INTO " + TABLE_ALL_TYPES_INSERT + " (" +
                "key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal," +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                ") VALUES (" +
                "'key2', null, null, null, null, null, null, null, null," +
                "null, null, null, null, null, null, null, null, null)");
        sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                " FROM " + TABLE_ALL_TYPES_INSERT + " WHERE key = 'key2'";
        result = execute(sql);
        rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key2", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));

        // insert into only a subset of columns
        execute("INSERT INTO " + TABLE_ALL_TYPES_INSERT + " (" +
                "key, typeinteger, typeansi, typeboolean) VALUES (" +
                "'key3', 999, 'ansi', false)");
        sql = "SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, " +
                "typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset" +
                " FROM " + TABLE_ALL_TYPES_INSERT + " WHERE key = 'key3'";
        result = execute(sql);
        rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key3", null, 999, null, null, null, "ansi", false, null, null, null, null, null, null, null, null, null, null));
    }

    private void assertSelect(String tableName, boolean createdByPresto)
    {
        Type uuidType = createdByPresto ? createUnboundedVarcharType() : createVarcharType(36);
        Type inetType = createdByPresto ? createUnboundedVarcharType() : createVarcharType(45);

        String sql = "SELECT " +
                " key, " +
                " typeuuid, " +
                " typeinteger, " +
                " typelong, " +
                " typebytes, " +
                " typetimestamp, " +
                " typeansi, " +
                " typeboolean, " +
                " typedecimal, " +
                " typedouble, " +
                " typefloat, " +
                " typeinet, " +
                " typevarchar, " +
                " typevarint, " +
                " typetimeuuid, " +
                " typelist, " +
                " typemap, " +
                " typeset " +
                " FROM " + tableName;

        MaterializedResult result = execute(sql);

        int rowCount = result.getRowCount();
        assertEquals(rowCount, 9);
        assertEquals(result.getTypes(), ImmutableList.of(
                createUnboundedVarcharType(),
                uuidType,
                INTEGER,
                BIGINT,
                VARBINARY,
                TIMESTAMP,
                createUnboundedVarcharType(),
                BOOLEAN,
                DOUBLE,
                DOUBLE,
                REAL,
                inetType,
                createUnboundedVarcharType(),
                createUnboundedVarcharType(),
                uuidType,
                createUnboundedVarcharType(),
                createUnboundedVarcharType(),
                createUnboundedVarcharType()));

        List<MaterializedRow> sortedRows = result.getMaterializedRows().stream()
                .sorted((o1, o2) -> o1.getField(1).toString().compareTo(o2.getField(1).toString()))
                .collect(toList());

        for (int rowNumber = 1; rowNumber <= rowCount; rowNumber++) {
            assertEquals(sortedRows.get(rowNumber - 1), new MaterializedRow(DEFAULT_PRECISION,
                    "key " + rowNumber,
                    String.format("00000000-0000-0000-0000-%012d", rowNumber),
                    rowNumber,
                    rowNumber + 1000L,
                    ByteBuffer.wrap(toByteArray(rowNumber)),
                    TIMESTAMP_LOCAL,
                    "ansi " + rowNumber,
                    rowNumber % 2 == 0,
                    Math.pow(2, rowNumber),
                    Math.pow(4, rowNumber),
                    (float) Math.pow(8, rowNumber),
                    "127.0.0.1",
                    "varchar " + rowNumber,
                    BigInteger.TEN.pow(rowNumber).toString(),
                    String.format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber),
                    String.format("[\"list-value-1%1$d\",\"list-value-2%1$d\"]", rowNumber),
                    String.format("{%d:%d,%d:%d}", rowNumber, rowNumber + 1L, rowNumber + 2, rowNumber + 3L),
                    "[false,true]"));
        }
    }

    private MaterializedResult execute(String sql)
    {
        return getQueryRunner().execute(SESSION, sql);
    }
}
