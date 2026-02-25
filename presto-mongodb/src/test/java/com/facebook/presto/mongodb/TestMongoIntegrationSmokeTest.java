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
package com.facebook.presto.mongodb;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import static com.facebook.presto.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestMongoIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private MongoQueryRunner mongoQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMongoQueryRunner(ORDERS);
    }

    @BeforeClass
    public void setUp()
    {
        mongoQueryRunner = (MongoQueryRunner) getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (mongoQueryRunner != null) {
            mongoQueryRunner.shutdown();
        }
    }

    @Test
    public void createTableWithEveryType()
    {
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 3.14E0 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", ObjectId('ffffffffffffffffffffffff') _objectid" +
                ", cast(ObjectId('ffffffffffffffffffffffff') as varchar) _objectid_string" +
                ", JSON '{\"name\":\"alice\"}' _json";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), true);
        assertEquals(row.getField(5), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(6), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), "ffffffffffffffffffffffff");
        assertEquals(row.getField(9), "{\"name\":\"alice\"}");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testInsertWithEveryType()
            throws Exception
    {
        String createSql = "" +
                "CREATE TABLE test_insert_types_table " +
                "(" +
                "  vc varchar" +
                ", vb varbinary" +
                ", bi bigint" +
                ", d double" +
                ", b boolean" +
                ", dt  date" +
                ", ts  timestamp" +
                ", tm time" +
                ", objid objectid" +
                ", _json json" +
                ")";
        getQueryRunner().execute(getSession(), createSql);

        String insertSql = "" +
                "INSERT INTO test_insert_types_table " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 3.14E0 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", TIME '11:22:33.456' _time" +
                ", ObjectId('ffffffffffffffffffffffff') _objectid" +
                ", JSON '{\"name\":\"alice\"}' _json";
        getQueryRunner().execute(getSession(), insertSql);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_insert_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), true);
        assertEquals(row.getField(5), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(6), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(7), LocalTime.of(11, 22, 33, 456_000_000));
        assertEquals(new ObjectId((byte[]) row.getField(8)), new ObjectId("ffffffffffffffffffffffff"));
        assertEquals(row.getField(9).toString(), "{\"name\":\"alice\"}");
        assertUpdate("DROP TABLE test_insert_types_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_insert_types_table"));
    }

    @Test
    public void testArrays()
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");
    }

    @Test
    public void testTemporalArrays()
    {
        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array7");
        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array8");
    }

    @Test
    public void testMaps()
    {
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[1.0E0], ARRAY[2.5E0]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map3", "SELECT 'kittens'");

        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", "SELECT 1");
        assertQuery("SELECT col[TRUE] FROM tmp_map4", "SELECT FALSE");

        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map6");
        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map7");

        assertUpdate("CREATE TABLE test.tmp_map8 (col MAP<VARCHAR, VARCHAR>)");
        mongoQueryRunner.getMongoClient().getDatabase("test").getCollection("tmp_map8").insertOne(new Document(
                ImmutableMap.of("col", new Document(ImmutableMap.of("key1", "value1", "key2", "value2")))));
        assertQuery("SELECT col['key1'] FROM test.tmp_map8", "SELECT 'value1'");

        assertUpdate("CREATE TABLE test.tmp_map9 (col VARCHAR)");
        mongoQueryRunner.getMongoClient().getDatabase("test").getCollection("tmp_map9").insertOne(new Document(
                ImmutableMap.of("col", new Document(ImmutableMap.of("key1", "value1", "key2", "value2")))));
        assertQuery("SELECT col FROM test.tmp_map9", "SELECT '{\"key1\": \"value1\", \"key2\": \"value2\"}'");

        assertUpdate("CREATE TABLE test.tmp_map10 (col VARCHAR)");
        mongoQueryRunner.getMongoClient().getDatabase("test").getCollection("tmp_map10").insertOne(new Document(
                ImmutableMap.of("col", ImmutableList.of(new Document(ImmutableMap.of("key1", "value1", "key2", "value2")),
                        new Document(ImmutableMap.of("key3", "value3", "key4", "value4"))))));
        assertQuery("SELECT col FROM test.tmp_map10", "SELECT '[{\"key1\": \"value1\", \"key2\": \"value2\"}, {\"key3\": \"value3\", \"key4\": \"value4\"}]'");

        assertUpdate("CREATE TABLE test.tmp_map11 (col VARCHAR)");
        mongoQueryRunner.getMongoClient().getDatabase("test").getCollection("tmp_map11").insertOne(new Document(
                ImmutableMap.of("col", 10)));
        assertQuery("SELECT col FROM test.tmp_map11", "SELECT '10'");

        assertUpdate("CREATE TABLE test.tmp_map12 (col VARCHAR)");
        mongoQueryRunner.getMongoClient().getDatabase("test").getCollection("tmp_map12").insertOne(new Document(
                ImmutableMap.of("col", Arrays.asList(10, null, 11))));
        assertQuery("SELECT col FROM test.tmp_map12", "SELECT '[10, null, 11]'");
    }

    @Test
    public void testCollectionNameContainsDots()
    {
        assertUpdate("CREATE TABLE \"tmp.dot1\" AS SELECT 'foo' _varchar", 1);
        assertQuery("SELECT _varchar FROM \"tmp.dot1\"", "SELECT 'foo'");
        assertUpdate("DROP TABLE \"tmp.dot1\"");
    }

    @Test
    public void testObjectIds()
    {
        assertUpdate("CREATE TABLE tmp_objectid AS SELECT ObjectId('ffffffffffffffffffffffff') AS id", 1);
        assertOneNotNullResult("SELECT id FROM tmp_objectid WHERE id = ObjectId('ffffffffffffffffffffffff')");
    }

    @Test
    public void testBinarys()
    {
        assertUpdate("CREATE TABLE tmp_binary AS SELECT cast('value' as varbinary) AS _varbinary", 1);
        assertOneNotNullResult("SELECT _varbinary FROM tmp_binary");

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT _varbinary FROM tmp_binary").toTestTypes();
        assertEquals(results.getRowCount(), 1);

        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "value".getBytes(UTF_8));
    }

    private void assertOneNotNullResult(String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }

    @Test
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE test_rename.tmp_rename_table (value bigint)");
        MongoCollection<Document> collection = mongoQueryRunner.getMongoClient().getDatabase("test_rename").getCollection("tmp_rename_table");
        collection.insertOne(new Document(ImmutableMap.of("value", 1)));
        assertQuery("SHOW TABLES IN test_rename", "SELECT 'tmp_rename_table'");
        assertQuery("SELECT value FROM test_rename.tmp_rename_table", "SELECT 1");

        assertUpdate("ALTER TABLE test_rename.tmp_rename_table RENAME TO test_rename.tmp_rename_new_table");

        assertQuery("SHOW TABLES IN test_rename", "SELECT 'tmp_rename_new_table'");
        assertQuery("SELECT value FROM test_rename.tmp_rename_new_table", "SELECT 1");
        assertUpdate("DROP TABLE test_rename.tmp_rename_new_table");
    }

    @Test
    public void testAlterTable()
    {
        assertUpdate("CREATE TABLE test_alter.tmp_alter_table (value bigint)");
        MongoCollection<Document> collection = mongoQueryRunner.getMongoClient().getDatabase("test_alter").getCollection("tmp_alter_table");
        collection.insertOne(new Document(ImmutableMap.of("value", 1)));

        assertUpdate("ALTER TABLE test_alter.tmp_alter_table ADD COLUMN email varchar");
        collection.insertOne(new Document(ImmutableMap.of("value", 2, "email", "example@example.com")));
        assertQuery("SELECT email from test_alter.tmp_alter_table WHERE email IS NOT NULL", "SELECT 'example@example.com'");
        assertUpdate("ALTER TABLE test_alter.tmp_alter_table RENAME COLUMN email TO email_id");
        assertQuery("SELECT email_id from test_alter.tmp_alter_table WHERE email_id IS NOT NULL", "SELECT 'example@example.com'");
        assertUpdate("ALTER TABLE test_alter.tmp_alter_table DROP COLUMN email_id");
    }

    @Test
    public void testJson()
    {
        assertUpdate("CREATE TABLE test_json (id INT, col JSON)");

        assertUpdate("INSERT INTO test_json VALUES (1, JSON '{\"name\":\"alice\"}')", 1);
        assertQuery("SELECT json_extract_scalar(col, '$.name') FROM test_json WHERE id = 1", "SELECT 'alice'");

        assertUpdate("INSERT INTO test_json VALUES (2, JSON '{\"numbers\":[1, 2, 3]}')", 1);
        assertQuery("SELECT json_extract(col, '$.numbers[0]') FROM test_json WHERE id = 2", "SELECT 1");

        assertUpdate("INSERT INTO test_json VALUES (3, NULL)", 1);
        assertQuery("SELECT col FROM test_json WHERE id = 3", "SELECT NULL");

        assertQueryFails(
                "CREATE TABLE test_json_scalar AS SELECT JSON '1' AS col",
                "Can't convert json to MongoDB Document.*");

        assertQueryFails(
                "CREATE TABLE test_json_array AS SELECT JSON '[\"a\", \"b\", \"c\"]' AS col",
                "Can't convert json to MongoDB Document.*");

        assertUpdate("DROP TABLE test_json");
    }
}
