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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestMongoViews
        extends AbstractTestQueryFramework
{
    private static GenericContainer<?> mongoContainer;
    private static MongoClient mongoClient;
    private static boolean initialized;
    private final int mongoContainerInternalPort = 27017;

    private synchronized void ensureContainerStarted()
    {
        if (!initialized) {
            mongoContainer = new GenericContainer<>(DockerImageName.parse("mongo:5.0"))
                    .withExposedPorts(27017)
                    .withCommand("mongod", "--bind_ip_all");
            mongoContainer.start();

            String host = mongoContainer.getContainerIpAddress();
            Integer port = mongoContainer.getMappedPort(27017);

            mongoClient = new MongoClient(host, port);

            initialized = true;
        }
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
        if (mongoContainer != null) {
            mongoContainer.stop();
            mongoContainer = null;
        }
        initialized = false;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        ensureContainerStarted();

        String mongoUrl = mongoContainer.getHost() + ":" + mongoContainer.getMappedPort(mongoContainerInternalPort);

        Session session = testSessionBuilder()
                .setCatalog("mongodb")
                .setSchema("test")
                .build();

        QueryRunner queryRunner = com.facebook.presto.tests.DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("mongodb.seeds", mongoUrl)
                .put("mongodb.schema-collection", "_schema")
                .build();

        queryRunner.installPlugin(new MongoPlugin());
        queryRunner.createCatalog("mongodb", "mongodb", properties);

        return queryRunner;
    }

    private MongoDatabase getTestDatabase()
    {
        return mongoClient.getDatabase("test");
    }

    @Test
    public void testQueryView()
    {
        assertUpdate("CREATE TABLE test.test_json (id INT, col VARCHAR)");
        assertUpdate("INSERT INTO test.test_json VALUES (1, 'alice'), (2, 'bob')", 2);

        MongoDatabase database = getTestDatabase();

        database.runCommand(new Document("create", "test_view")
                .append("viewOn", "test_json")
                .append("pipeline", Arrays.asList(
                        new Document("$project", new Document()
                                .append("id", 1)
                                .append("col", 1)))));

        boolean foundView = false;
        for (Document doc : database.listCollections()) {
            if ("test_view".equals(doc.getString("name"))) {
                assertEquals(doc.getString("type"), "view", "test_view should be a view, not a collection");
                foundView = true;
                break;
            }
        }
        assertEquals(foundView, true, "test_view should exist");

        assertQuery("SELECT * FROM test.test_view ORDER BY id",
                "VALUES (1, 'alice'), (2, 'bob')");

        assertQuery("SELECT col FROM test.test_view WHERE id = 1",
                "VALUES ('alice')");

        assertUpdate("DROP TABLE test.test_json");
        database.getCollection("test_view").drop();
    }

    @Test
    public void testViewWithAggregation()
    {
        assertUpdate("CREATE TABLE test.sales (product VARCHAR, quantity INT, price DOUBLE)");
        assertUpdate("INSERT INTO test.sales VALUES " +
                "('apple', 10, 1.5), " +
                "('banana', 20, 0.5), " +
                "('apple', 5, 1.5)", 3);

        MongoDatabase database = getTestDatabase();

        database.runCommand(new Document("create", "sales_summary")
                .append("viewOn", "sales")
                .append("pipeline", Arrays.asList(
                        new Document("$group", new Document()
                                .append("_id", "$product")
                                .append("total_quantity", new Document("$sum", "$quantity"))
                                .append("avg_price", new Document("$avg", "$price"))),
                        new Document("$project", new Document()
                                .append("product", "$_id")
                                .append("total_quantity", 1)
                                .append("avg_price", 1)
                                .append("_id", 0)))));

        assertQuery("SELECT product, total_quantity FROM test.sales_summary WHERE product = 'apple'",
                "VALUES ('apple', 15)");

        assertUpdate("DROP TABLE test.sales");
        database.getCollection("sales_summary").drop();
    }

    @Test
    public void testViewDoesNotHaveIndexes()
    {
        assertUpdate("CREATE TABLE test.indexed_table (id INT, name VARCHAR)");
        assertUpdate("INSERT INTO test.indexed_table VALUES (1, 'test')", 1);

        MongoDatabase database = getTestDatabase();

        database.runCommand(new Document("create", "indexed_view")
                .append("viewOn", "indexed_table")
                .append("pipeline", Arrays.asList(
                        new Document("$project", new Document()
                                .append("id", 1)
                                .append("name", 1)))));

        assertQuery("SELECT * FROM test.indexed_view", "VALUES (1, 'test')");

        boolean isView = false;
        for (Document doc : database.listCollections()) {
            if ("indexed_view".equals(doc.getString("name"))) {
                isView = "view".equals(doc.getString("type"));
                break;
            }
        }
        assertEquals(isView, true, "indexed_view should be a view");

        assertUpdate("DROP TABLE test.indexed_table");
        database.getCollection("indexed_view").drop();
    }

    @Test
    public void testViewWithFilter()
    {
        assertUpdate("CREATE TABLE test.all_items (id INT, category VARCHAR, active BOOLEAN)");
        assertUpdate("INSERT INTO test.all_items VALUES " +
                "(1, 'A', true), " +
                "(2, 'B', false), " +
                "(3, 'A', true), " +
                "(4, 'C', true)", 4);

        MongoDatabase database = getTestDatabase();

        database.runCommand(new Document("create", "active_a_items")
                .append("viewOn", "all_items")
                .append("pipeline", Arrays.asList(
                        new Document("$match", new Document()
                                .append("category", "A")
                                .append("active", true)),
                        new Document("$project", new Document()
                                .append("id", 1)
                                .append("category", 1)
                                .append("active", 1)))));

        assertQuery("SELECT id FROM test.active_a_items ORDER BY id",
                "VALUES (1), (3)");

        assertUpdate("DROP TABLE test.all_items");
        database.getCollection("active_a_items").drop();
    }
}
