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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import org.bson.Document;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.facebook.presto.common.predicate.Range.equal;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.Range.greaterThanOrEqual;
import static com.facebook.presto.common.predicate.Range.lessThan;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMongoSession
{
    private static final MongoColumnHandle COL1 = new MongoColumnHandle("col1", BIGINT, false);
    private static final MongoColumnHandle COL2 = new MongoColumnHandle("col2", createUnboundedVarcharType(), false);
    private static final MongoColumnHandle COL3 = new MongoColumnHandle("col3", VARBINARY, false);

    private MongoServer server;
    private MongoClient client;
    private MongoSession session;
    private TypeManager typeManager;

    @Test
    public void testBuildQuery()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(range(BIGINT, 100L, false, 200L, true)), false),
                COL2, Domain.singleValue(createUnboundedVarcharType(), utf8Slice("a value"))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL1.getName(), new Document().append("$gt", 100L).append("$lte", 200L))
                .append(COL2.getName(), new Document("$eq", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryBinaryType()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL3, Domain.singleValue(VARBINARY, wrappedBuffer("VarBinary Value".getBytes(UTF_8)))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL3.getName(), new Document().append("$eq", "VarBinary Value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryStringType()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("hello"), false, utf8Slice("world"), true)), false),
                COL2, Domain.create(ValueSet.ofRanges(greaterThanOrEqual(createUnboundedVarcharType(), utf8Slice("a value"))), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL1.getName(), new Document().append("$gt", "hello").append("$lte", "world"))
                .append(COL2.getName(), new Document("$gte", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryIn()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL2, Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("hello")), equal(createUnboundedVarcharType(), utf8Slice("world"))), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document(COL2.getName(), new Document("$in", ImmutableList.of("hello", "world")));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryOr()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 100L), greaterThan(BIGINT, 200L)), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getName(), new Document("$lt", 100L)),
                new Document(COL1.getName(), new Document("$gt", 200L))));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryNull()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 200L)), true)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getName(), new Document("$gt", 200L)),
                new Document(COL1.getName(), new Document("$exists", true).append("$eq", null))));
        assertEquals(query, expected);
    }

    @BeforeClass
    public void setupMongoServer()
    {
        server = new MongoServer(new SyncMemoryBackend());
        InetSocketAddress address = server.bind();
        client = new MongoClient(new ServerAddress(address));
        typeManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();

        MongoClientConfig config = new MongoClientConfig();
        session = new MongoSession(typeManager, client, config);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownMongoServer()
    {
        if (session != null) {
            session.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    public void testGetAllTablesAfterTableRename()
    {
        String schemaName = "test_table_rename";
        MongoDatabase database = client.getDatabase(schemaName);

        // Create table A with data
        database.getCollection("table_a").insertOne(new Document("field1", "value1"));

        // Create metadata for table A
        MongoCollection<Document> schemaCollection = database.getCollection("_schema");
        schemaCollection.insertOne(new Document("table", "table_a")
                .append("fields", ImmutableList.of(
                        new Document("name", "field1").append("type", "varchar").append("hidden", false))));

        // Verify table A exists
        Set<String> tablesBeforeRename = session.getAllTables(schemaName);
        assertEquals(tablesBeforeRename.size(), 1);
        assertTrue(tablesBeforeRename.contains("table_a"));
        assertFalse(tablesBeforeRename.contains("table_b"));

        // Rename collection from table_a to table_b in MongoDB backend
        database.getCollection("table_a").renameCollection(new com.mongodb.MongoNamespace(schemaName, "table_b"));

        // Update metadata to reflect the rename
        schemaCollection.updateOne(
                new Document("table", "table_a"),
                new Document("$set", new Document("table", "table_b")));

        // Get all tables after rename - should return table_b only
        Set<String> tablesAfterRename = session.getAllTables(schemaName);

        // Verify only table_b is returned
        assertEquals(tablesAfterRename.size(), 1);
        assertTrue(tablesAfterRename.contains("table_b"));
        assertFalse(tablesAfterRename.contains("table_a"));

        // Verify metadata reflects the new table name
        Document metadata = schemaCollection.find(new Document("table", "table_b")).first();
        assertTrue(metadata != null, "Metadata for table_b should exist");
        assertEquals(metadata.getString("table"), "table_b");

        // Verify old table name metadata doesn't exist
        Document oldMetadata = schemaCollection.find(new Document("table", "table_a")).first();
        assertEquals(oldMetadata, null, "Metadata for table_a should not exist");

        database.drop();
    }

    @Test
    public void testGetAllTablesWithStaleMetadata()
    {
        String schemaName = "test_stale_metadata";
        MongoDatabase database = client.getDatabase(schemaName);

        // Create a collection with data
        MongoCollection<Document> collection = database.getCollection("active_table");
        collection.insertOne(new Document("field1", "value1"));

        // Create metadata for both active and non-existent tables
        MongoCollection<Document> schemaCollection = database.getCollection("_schema");
        schemaCollection.insertOne(new Document("table", "active_table")
                .append("fields", ImmutableList.of(
                        new Document("name", "field1").append("type", "varchar").append("hidden", false))));
        schemaCollection.insertOne(new Document("table", "stale_table")
                .append("fields", ImmutableList.of(
                        new Document("name", "field1").append("type", "varchar").append("hidden", false))));

        // Get all tables - should only return active_table and clean up stale_table metadata
        Set<String> tables = session.getAllTables(schemaName);

        // Verify only active_table is returned
        assertEquals(tables.size(), 1);
        assertTrue(tables.contains("active_table"));
        assertFalse(tables.contains("stale_table"));

        // Verify stale metadata was removed
        Document staleMetadata = schemaCollection.find(new Document("table", "stale_table")).first();
        assertEquals(staleMetadata, null, "Stale metadata should have been deleted");

        // Verify active metadata still exists
        Document activeMetadata = schemaCollection.find(new Document("table", "active_table")).first();
        assertTrue(activeMetadata != null, "Active metadata should still exist");

        database.drop();
    }

    @Test
    public void testGetAllTablesWithStaleMetadataAfterDirectRename()
    {
        String schemaName = "test_direct_rename_stale_metadata";
        MongoDatabase database = client.getDatabase(schemaName);

        //  Create collection table_a with data
        MongoCollection<Document> tableA = database.getCollection("table_a");
        tableA.insertOne(new Document("field1", "value1"));

        //  Create _schema metadata for table_a
        MongoCollection<Document> schemaCollection = database.getCollection("_schema");
        schemaCollection.insertOne(new Document("table", "table_a")
                .append("fields", ImmutableList.of(
                        new Document("name", "field1").append("type", "varchar").append("hidden", false))));

        //  Optionally call getAllTables once to warm any caches
        Set<String> tablesBeforeRename = session.getAllTables(schemaName);
        assertEquals(tablesBeforeRename.size(), 1);
        assertTrue(tablesBeforeRename.contains("table_a"));

        // Rename table_a to table_b in MongoDB WITHOUT updating _schema
        // This simulates a direct MongoDB operation that leaves stale metadata
        tableA.renameCollection(new MongoNamespace(schemaName, "table_b"));

        // At this point:
        // - MongoDB has collection "table_b" (not "table_a")
        // - _schema still has metadata for "table_a" (stale)
        // - _schema does NOT have metadata for "table_b"

        // Step 5: Call getAllTables again - this should trigger stale metadata cleanup
        Set<String> tablesAfterRename = session.getAllTables(schemaName);

        // Step 6: Assert the result includes table_b but not table_a
        assertEquals(tablesAfterRename.size(), 1, "Should return exactly one table");
        assertTrue(tablesAfterRename.contains("table_b"), "Should include table_b");
        assertFalse(tablesAfterRename.contains("table_a"), "Should not include table_a");

        // Step 7: Assert _schema no longer contains a document for table_a
        Document staleMetadata = schemaCollection.find(new Document("table", "table_a")).first();
        assertNull(staleMetadata, "Stale metadata for table_a should have been deleted");

        // Verify table_b collection actually exists
        boolean tableBExists = StreamSupport.stream(database.listCollectionNames().spliterator(), false)
                .anyMatch("table_b"::equals);
        assertTrue(tableBExists, "Collection table_b should exist in MongoDB");

        // Step 8: Verify getTable behavior after stale metadata cleanup
        SchemaTableName oldTable = new SchemaTableName(schemaName, "table_a");

        // Original table_a metadata should be purged and not accessible
        // This verifies that the cache was properly invalidated and metadata was deleted
        expectThrows(TableNotFoundException.class, () -> session.getTable(oldTable));

        database.drop();
    }
}
