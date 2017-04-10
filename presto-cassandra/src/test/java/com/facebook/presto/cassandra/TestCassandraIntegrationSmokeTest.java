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

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import static com.datastax.driver.core.utils.Bytes.toRawHexString;
import static com.facebook.presto.cassandra.CassandraQueryRunner.createCassandraSession;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES_PARTITION_KEY;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS_LARGE;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_MULTI_PARTITION_CLUSTERING_KEYS;
import static com.facebook.presto.cassandra.CassandraTestingUtils.createTestTables;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.google.common.primitives.Ints.toByteArray;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCassandraIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final String KEYSPACE = "smoke_test";
    private static final Session SESSION = createCassandraSession(KEYSPACE);

    private static final DateTime DATE_TIME_UTC = new DateTime(1970, 1, 1, 3, 4, 5, UTC);
    private static final Date DATE_LOCAL = new Date(DATE_TIME_UTC.getMillis());
    private static final Timestamp TIMESTAMP_LOCAL = new Timestamp(DATE_TIME_UTC.getMillis());

    public TestCassandraIntegrationSmokeTest()
            throws Exception
    {
        super(CassandraQueryRunner::createCassandraQueryRunner);
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        createTestTables(EmbeddedCassandra.getSession(), KEYSPACE, DATE_LOCAL);
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
                " AND typetimestamp = TIMESTAMP '1970-01-01 03:04:05'" +
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
            throws Exception
    {
        assertSelect(TABLE_ALL_TYPES, false);
        assertSelect(TABLE_ALL_TYPES_PARTITION_KEY, false);
    }

    @Test
    public void testCreateTableAs()
            throws Exception
    {
        execute("DROP TABLE IF EXISTS table_all_types_copy");
        execute("CREATE TABLE table_all_types_copy AS SELECT * FROM " + TABLE_ALL_TYPES);
        assertSelect("table_all_types_copy", true);
        execute("DROP TABLE table_all_types_copy");
    }

    @Test
    public void testClusteringPredicates()
            throws Exception
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
            throws Exception
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

    @Test
    public void testClusteringKeyOnlyPushdown()
            throws Exception
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
                createUnboundedVarcharType()
        ));

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
                    "[false,true]"
            ));
        }
    }

    private MaterializedResult execute(String sql)
    {
        return getQueryRunner().execute(SESSION, sql);
    }
}
