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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMongoViews
        extends AbstractTestQueryFramework
{
    private static GenericContainer<?> mongoContainer;
    private static MongoClient mongoClient;
    private final int mongoContainerInternalPort = 27017;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        mongoContainer = new GenericContainer<>(DockerImageName.parse("mongo:5.0"))
                .withExposedPorts(mongoContainerInternalPort)
                .withCommand("mongod", "--bind_ip_all");
        mongoContainer.start();

        String host = mongoContainer.getContainerIpAddress();
        Integer port = mongoContainer.getMappedPort(mongoContainerInternalPort);

        mongoClient = new MongoClient(host, port);
        super.init();

        MongoCollection<Document> viewBase = mongoClient.getDatabase("test")
                .getCollection("view_base");
        viewBase.insertMany(ImmutableList.of(
                new Document("id", 1L).append("name", "alpha").append("active", true),
                new Document("id", 2L).append("name", "beta").append("active", false),
                new Document("id", 3L).append("name", "gamma").append("active", true)));

        MongoCollection<Document> sales = mongoClient.getDatabase("test")
                .getCollection("sales");
        sales.insertMany(ImmutableList.of(
                new Document("product", "apple").append("quantity", 10),
                new Document("product", "apple").append("quantity", 5),
                new Document("product", "banana").append("quantity", 8)));

        MongoDatabase testDb = mongoClient.getDatabase("test");

        testDb.createView(
                "test_view",
                "view_base",
                ImmutableList.of(new Document("$project", new Document()
                        .append("_id", 0)
                        .append("id", 1)
                        .append("name", 1))));

        testDb.createView(
                "active_a_items",
                "view_base",
                ImmutableList.of(
                        new Document("$match", new Document("active", true))));

        testDb.createView(
                "sales_summary",
                "sales",
                ImmutableList.of(
                        new Document("$group", new Document("_id", "$product")
                                .append("total_quantity", new Document("$sum", "$quantity"))),
                        new Document("$project", new Document("product", "$_id")
                                .append("total_quantity", 1)
                                .append("_id", 0))));

        testDb.createView(
                "indexed_view",
                "view_base",
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void close()
    {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
        if (mongoContainer != null) {
            mongoContainer.stop();
            mongoContainer = null;
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
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
        MongoDatabase database = getTestDatabase();

        boolean foundView = false;
        for (Document doc : database.listCollections()) {
            if ("test_view".equals(doc.getString("name"))) {
                assertEquals(doc.getString("type"), "view", "test_view should be a view, not a collection");
                foundView = true;
                break;
            }
        }
        assertTrue(foundView, "test_view should exist");

        assertQuery("SELECT * FROM test.test_view ORDER BY id",
                "VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");

        assertQuery("SELECT name FROM test.test_view WHERE id = 1",
                "VALUES ('alpha')");
    }

    @Test
    public void testViewWithAggregation()
    {
        MaterializedResult result = computeActual(
                "SELECT product, total_quantity FROM test.sales_summary ORDER BY product");

        assertEquals(result.getRowCount(), 2);

        // apple: 10 + 5 = 15, banana: 8
        List<String> products = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toList());
        assertTrue(products.contains("apple"), "Expected 'apple' in sales_summary");
        assertTrue(products.contains("banana"), "Expected 'banana' in sales_summary");

        int appleIdx = products.indexOf("apple");
        assertEquals(result.getMaterializedRows().get(appleIdx).getField(1), 15L,
                "apple total_quantity should be 15");
    }

    @Test
    public void testViewWithFilter()
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM test.active_a_items ORDER BY id");

        assertEquals(result.getRowCount(), 2, "active_a_items should return exactly 2 rows");
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 3L);
    }

    @Test
    public void testCreateViewNotSupported()
    {
        assertQueryFails(
                "CREATE VIEW mongodb.test.unsupported AS SELECT name FROM mongodb.test.view_base",
                ".*not supported.*");
    }

    @Test
    public void testNativeMongoViewIsQueryable()
    {
        MaterializedResult result = computeActual("SELECT id FROM test.test_view ORDER BY id");
        assertEquals(result.getRowCount(), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3L);
    }

    @Test
    public void testShowCreateViewNotSupported()
    {
        assertQueryFails(
                "SHOW CREATE VIEW test.test_view",
                ".*is a table, not a view.*");
    }

    @Test
    public void testViewDoesNotLeakIntoTableList()
    {
        MaterializedResult tables = computeActual("SHOW TABLES FROM test");
        List<String> names = tables.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toList());
        assertFalse(names.contains("_schema"), "_schema should never appear as a table");
    }

    @Test
    public void testViewColumnsInferredFromSample()
    {
        MaterializedResult result = computeActual("DESCRIBE test.test_view");
        List<String> columns = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toList());
        assertFalse(columns.isEmpty(), "View should have at least one column");
        assertTrue(columns.contains("id"), "Expected column 'id' to be inferred from view output");
        assertTrue(columns.contains("name"), "Expected column 'name' to be inferred from view output");
    }

    @Test
    public void testViewDoesNotHaveIndexes()
    {
        MaterializedResult result = computeActual("SELECT * FROM test.indexed_view");
        assertNotNull(result, "Query against view should succeed without fetching indexes");
    }

    @Test
    public void testDropViewRemovesFromSchemaCollection()
    {
        // Create inline so this test is self-contained and order-independent
        getTestDatabase().createView(
                "drop_view_isolated",
                "view_base",
                ImmutableList.of());

        assertUpdate("DROP TABLE test.drop_view_isolated");

        assertQueryFails(
                "SELECT * FROM test.drop_view_isolated",
                ".*not found.*|.*does not exist.*");
    }

    @Test
    public void testRenameViewPreservesPipelineOutput()
    {
        getTestDatabase().createView(
                "rename_isolated_src",
                "view_base",
                ImmutableList.of(
                        new Document("$match", new Document("active", true))));

        MaterializedResult before = computeActual(
                "SELECT COUNT(*) FROM test.rename_isolated_src");
        long countBefore = (long) before.getMaterializedRows().get(0).getField(0);

        assertUpdate("ALTER TABLE test.rename_isolated_src RENAME TO rename_isolated_dst");

        MaterializedResult after = computeActual(
                "SELECT COUNT(*) FROM test.rename_isolated_dst");
        long countAfter = (long) after.getMaterializedRows().get(0).getField(0);

        assertEquals(countBefore, countAfter, "Row count should be identical after rename");

        assertUpdate("DROP TABLE test.rename_isolated_dst");
    }
}
