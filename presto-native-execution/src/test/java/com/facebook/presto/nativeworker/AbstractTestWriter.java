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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createEmptyTable;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartitionedNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPrestoBenchTables;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestWriter
        extends AbstractTestQueryFramework
{
    private static final String[] TABLE_FORMATS = {"DWRF"};

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createCustomer(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createSupplier(queryRunner);
        createBucketedCustomer(queryRunner);
        createPart(queryRunner);
        createRegion(queryRunner);
        createEmptyTable(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);
        createPrestoBenchTables(queryRunner);
    }

    @Test
    public void testCreateTableWithUnsupportedFormats()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        String[] unsupportedTableFormats = {"ORC", "JSON"};
        for (String unsupportedTableFormat : unsupportedTableFormats) {
            assertQueryFails(String.format("CREATE TABLE %s WITH (format = '" + unsupportedTableFormat + "') AS SELECT * FROM nation", tmpTableName), " Unsupported file format in TableWrite: \"" + unsupportedTableFormat + "\".");
        }
    }

    @Test
    public void testCreateUnpartitionedTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        for (String tableFormat : TABLE_FORMATS) {
            try {
                getQueryRunner().execute(session, String.format("CREATE TABLE %s WITH (format = '" + tableFormat + "') AS SELECT * FROM nation", tmpTableName));
                assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT * FROM nation");
            }
            finally {
                dropTableIfExists(tmpTableName);
            }
        }

        try {
            getQueryRunner().execute(session, String.format("CREATE TABLE %s AS SELECT linenumber, count(*) as cnt FROM lineitem GROUP BY 1", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT linenumber, count(*) FROM lineitem GROUP BY 1");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }

        try {
            getQueryRunner().execute(session, String.format("CREATE TABLE %s AS SELECT orderkey, count(*) as cnt FROM lineitem GROUP BY 1", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT orderkey, count(*) FROM lineitem GROUP BY 1");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testCreatePartitionedTableAsSelect()
    {
        {
            Session session = buildSessionForTableWrite();
            // Generate temporary table name for created partitioned table.
            String partitionedOrdersTableName = generateRandomTableName();

            for (String tableFormat : TABLE_FORMATS) {
                try {
                    getQueryRunner().execute(session, String.format(
                            "CREATE TABLE %s WITH (format = '" + tableFormat + "', " +
                                    "partitioned_by = ARRAY[ 'orderstatus' ]) " +
                                    "AS SELECT custkey, comment, orderstatus FROM orders", partitionedOrdersTableName));
                    assertQuery(String.format("SELECT * FROM %s", partitionedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders");
                }
                finally {
                    dropTableIfExists(partitionedOrdersTableName);
                }
            }
        }
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        Session writeSession = buildSessionForTableWrite();

        try {
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT, nationkey BIGINT) WITH (partitioned_by = ARRAY['regionkey','nationkey'])", tmpTableName));
            // Test insert into an empty table.
            getQueryRunner().execute(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT name, regionkey, nationkey FROM nation");

            // Test failure on insert into existing partitions.
            assertQueryFails(writeSession, String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName),
                    ".*Cannot insert into an existing partition of Hive table: regionkey=.*/nationkey=.*");

            // Test insert into existing partitions if insert_existing_partitions_behavior is set to OVERWRITE.
            Session overwriteSession = Session.builder(writeSession)
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .build();
            getQueryRunner().execute(overwriteSession, String.format("INSERT INTO %s SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName), "SELECT CONCAT(name, '.test'), regionkey, nationkey FROM nation");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testInsertIntoSpecialPartitionName()
    {
        Session writeSession = buildSessionForTableWrite();
        // Generate temporary table name.
        String tmpTableName = generateRandomTableName();
        try {
            getQueryRunner().execute(writeSession, String.format("CREATE TABLE %s (name VARCHAR, nationkey VARCHAR) WITH (partitioned_by = ARRAY['nationkey'])", tmpTableName));

            // For special character in partition name, without correct handling, it would throw errors like 'Invalid partition spec: nationkey=A/B'
            // In this test, verify those partition names can be successfully created
            String[] specialCharacters = new String[] {"\"", "#", "%", "''", "*", "/", ":", "=", "?", "\\", "\\x7F", "{", "[", "]", "^"}; // escape single quote for sql
            for (String specialCharacter : specialCharacters) {
                getQueryRunner().execute(writeSession, String.format("INSERT INTO %s VALUES ('name', 'A%sB')", tmpTableName, specialCharacter));
                assertQuery(String.format("SELECT nationkey FROM %s", tmpTableName), String.format("VALUES('A%sB')", specialCharacter));
                getQueryRunner().execute(writeSession, String.format("DELETE FROM %s", tmpTableName));
            }
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testCreateBucketTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name for bucketed table.
        String bucketedOrdersTableName = generateRandomTableName();

        for (String tableFormat : TABLE_FORMATS) {
            try {
                getQueryRunner().execute(session, String.format(
                        "CREATE TABLE %s WITH (format = '" + tableFormat + "', " +
                                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                                "bucketed_by = ARRAY[ 'custkey' ], " +
                                "bucket_count = 2) " +
                                "AS SELECT custkey, comment, orderstatus FROM orders", bucketedOrdersTableName));
                assertQuery(String.format("SELECT * FROM %s", bucketedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders");
                assertQuery(String.format("SELECT * FROM %s where \"$bucket\" = 0", bucketedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders where custkey % 2 = 0");
                assertQuery(String.format("SELECT * FROM %s where \"$bucket\" = 1", bucketedOrdersTableName), "SELECT custkey, comment, orderstatus FROM orders where custkey % 2 = 1");
            }
            finally {
                dropTableIfExists(bucketedOrdersTableName);
            }
        }
    }

    @Test
    public void testCreateBucketSortedTableAsSelect()
    {
        Session session = buildSessionForTableWrite();
        // Generate temporary table name.
        String badBucketTableName = generateRandomTableName();

        for (String tableFormat : TABLE_FORMATS) {
            try {
                getQueryRunner().execute(session, String.format(
                        "CREATE TABLE %s WITH (format = '%s', " +
                                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                                "bucketed_by=array['custkey'], " +
                                "bucket_count=1, " +
                                "sorted_by=array['orderkey']) " +
                                "AS SELECT custkey, orderkey, orderstatus FROM orders", badBucketTableName, tableFormat));
                assertQueryOrdered(String.format("SELECT custkey, orderkey, orderstatus FROM %s where orderstatus = '0'", badBucketTableName), "SELECT custkey, orderkey, orderstatus FROM orders where orderstatus = '0'");
            }
            finally {
                dropTableIfExists(badBucketTableName);
            }
        }
    }

    @Test
    public void testScaleWriters()
    {
        Session session = buildSessionForTableWrite();
        String tmpTableName = generateRandomTableName();
        getQueryRunner().execute(session, String.format(
                "CREATE TABLE %s AS SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, CAST(o_orderdate as VARCHAR) as o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM tpchstandard.tiny.orders", tmpTableName));
        assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM " + tmpTableName).getOnlyValue(), 1L);
        dropTableIfExists(tmpTableName);

        tmpTableName = generateRandomTableName();
        getQueryRunner().execute(session, String.format(
                "CREATE TABLE %s AS SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, CAST(o_orderdate as VARCHAR) as o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM tpchstandard.sf100.orders where o_orderdate > Date('1997-01-10')", tmpTableName));
        long files = (long) computeActual("SELECT count(DISTINCT \"$path\") FROM " + tmpTableName).getOnlyValue();
        long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
        assertThat(files).isBetween(2L, workers);
        dropTableIfExists(tmpTableName);
    }

    @Test
    public void testCollectColumnStatisticsOnCreateTable()
    {
        Session session = buildSessionForTableWrite();
        String tmpTableName = generateRandomTableName();
        // TODO: add varbinary test support once velox supports varbinary in value node
        // https://github.com/prestodb/presto/blob/master/presto-native-execution/presto_cpp/main/types/PrestoToVeloxQueryPlan.cpp#L915
        assertUpdate(session, format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ") " +
                "AS " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), sequence(0, 10), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), sequence(10, 20), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), sequence(20, 25), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), sequence(30, 35), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar)", tmpTableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8.0
                        "('c_array', 184.0E0, null, 0.5, null, null, null, null), " + // 176
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar, h_varchar)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8
                        "('c_array', 104.0E0, null, 0.5, null, null, null, null), " + // 96
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar, h_varchar)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "('c_array', null, 0E0, 0E0, null, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null, null)) AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar, h_varchar)");

        dropTableIfExists(tmpTableName);
    }

    @Test
    public void testCollectColumnStatisticsOnInsert()
    {
        Session session = buildSessionForTableWrite();
        String tmpTableName = generateRandomTableName();
        assertUpdate(session, format("" +
                "CREATE TABLE %s ( " +
                "   c_boolean BOOLEAN, " +
                "   c_bigint BIGINT, " +
                "   c_double DOUBLE, " +
                "   c_timestamp TIMESTAMP, " +
                "   c_varchar VARCHAR, " +
                "   c_array ARRAY(BIGINT), " +
                "   p_varchar VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ")", tmpTableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), sequence(0, 10), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), sequence(10, 20), 'p1')," +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), sequence(20, 25), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), sequence(30, 35), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar)", tmpTableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8
                        "('c_array', 184.0E0, null, 0.5E0, null, null, null, null), " + // 176
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar, p_varchar)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 16.0E0, 2.0E0, 0.5E0, null, null, null, null), " + // 8
                        "('c_array', 104.0E0, null, 0.5, null, null, null, null), " + // 96
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)) AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar, p_varchar)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tmpTableName),
                "SELECT * FROM (VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "('c_array', null, 0E0, 0E0, null, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null, null)) AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_array, p_varchar, p_varchar)");

        dropTableIfExists(tmpTableName);
    }

    private void dropTableIfExists(String tableName)
    {
        computeExpected(String.format("DROP TABLE IF EXISTS %s", tableName), ImmutableList.of(BIGINT));
    }

    private String generateRandomTableName()
    {
        String tableName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
        // Clean up if the temporary named table already exists.
        dropTableIfExists(tableName);
        return tableName;
    }

    private Session buildSessionForTableWrite()
    {
        return Session.builder(getSession())
                .setSystemProperty("scale_writers", "true")
                .setSystemProperty("table_writer_merge_operator_enabled", "true")
                .setSystemProperty("task_writer_count", "1")
                .setSystemProperty("task_partitioned_writer_count", "2")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "true")
                .setCatalogSessionProperty("hive", "orc_compression_codec", "ZSTD")
                .build();
    }
}
