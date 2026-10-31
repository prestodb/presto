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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.OptionalInt;
import java.util.UUID;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Nested struct fields must remain readable when a Parquet data file carries no Iceberg field IDs
 * (for example files written by Hive or Spark and then registered with the table). Field-ID based
 * matching is only usable when IDs are present; otherwise the reader has to fall back to the
 * (hex-encoded) field names.
 */
public class TestIcebergParquetNestedFieldsWithoutFieldIds
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA_NAME = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(FileFormat.PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    @DataProvider(name = "pushdown")
    public Object[][] pushdown()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "pushdown")
    public void testNestedFieldsInFileWithoutFieldIds(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_no_field_ids_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Schema schema = new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "info", Types.StructType.of(
                            Types.NestedField.optional(3, "city", Types.StringType.get()),
                            Types.NestedField.optional(4, "zip", Types.IntegerType.get()),
                            Types.NestedField.optional(5, "geo", Types.StructType.of(
                                    Types.NestedField.optional(6, "lat", Types.IntegerType.get()))))));
            Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), ImmutableMap.of());

            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id;" +
                            " optional group info {" +
                            "  optional binary city (UTF8);" +
                            "  optional int32 zip;" +
                            "  optional group geo { optional int32 lat; }" +
                            " }" +
                            "}");
            SimpleGroupFactory factory = new SimpleGroupFactory(parquetSchema);
            Group row1 = factory.newGroup().append("id", 1);
            Group info1 = row1.addGroup("info");
            info1.append("city", "Pune").append("zip", 411001);
            info1.addGroup("geo").append("lat", 18);
            Group row2 = factory.newGroup().append("id", 2);
            Group info2 = row2.addGroup("info");
            info2.append("city", "Delhi").append("zip", 110001);
            info2.addGroup("geo").append("lat", 28);

            appendParquetFile(table, parquetSchema, row1, row2);

            Session session = pushdownSession(pushdown);
            assertQuery(session, "SELECT id, info.city, info.zip, info.geo.lat FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Pune', 411001, 18), (2, 'Delhi', 110001, 28)");
            assertQuery(session, "SELECT info.city FROM " + tableName + " WHERE info.zip = 110001",
                    "VALUES ('Delhi')");
            assertQuery(session, "SELECT info.geo.lat FROM " + tableName + " ORDER BY id",
                    "VALUES 18, 28");
            assertQuery(session, "SELECT count(*) FROM " + tableName + " WHERE info IS NOT NULL AND info.city IS NOT NULL",
                    "VALUES 2");
            // Whole-struct read exercises the non-pushdown field construction path.
            assertQuery(session, "SELECT info.city, info.zip FROM (SELECT info FROM " + tableName + ") ORDER BY 2",
                    "VALUES ('Delhi', 110001), ('Pune', 411001)");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test(dataProvider = "pushdown")
    public void testHyphenatedNestedFieldsInFileWithoutFieldIds(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_hyphen_no_field_ids_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Schema schema = new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "location", Types.StructType.of(
                            Types.NestedField.optional(3, "aws-region", Types.StringType.get()),
                            Types.NestedField.optional(4, "zone-id", Types.IntegerType.get()))));
            Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), ImmutableMap.of());

            // Parquet column names are hex-encoded the same way Iceberg's Parquet writer encodes them.
            String region = AvroSchemaUtil.makeCompatibleName("aws-region");
            String zone = AvroSchemaUtil.makeCompatibleName("zone-id");
            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id;" +
                            " optional group location {" +
                            "  optional binary " + region + " (UTF8);" +
                            "  optional int32 " + zone + ";" +
                            " }" +
                            "}");
            SimpleGroupFactory factory = new SimpleGroupFactory(parquetSchema);
            Group row1 = factory.newGroup().append("id", 1);
            row1.addGroup("location").append(region, "us-west-2").append(zone, 100);
            Group row2 = factory.newGroup().append("id", 2);
            row2.addGroup("location").append(region, "eu-central-1").append(zone, 200);

            appendParquetFile(table, parquetSchema, row1, row2);

            Session session = pushdownSession(pushdown);
            assertQuery(session, "SELECT id, location.\"aws-region\", location.\"zone-id\" FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'us-west-2', 100), (2, 'eu-central-1', 200)");
            assertQuery(session, "SELECT location.\"aws-region\" FROM " + tableName + " WHERE location.\"zone-id\" = 200",
                    "VALUES ('eu-central-1')");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * The group has field IDs, but the physical name ("old_city") differs from the requested name
     * ("city"). The stable field ID must win over the stale name.
     */
    @Test(dataProvider = "pushdown")
    public void testFieldIdWinsOverStalePhysicalName(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_stale_name_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Table table = createInfoTable(catalog, tableId);
            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id = 1;" +
                            " optional group info = 2 {" +
                            "  optional binary old_city (UTF8) = 3;" +
                            "  optional int32 zip = 4;" +
                            " }" +
                            "}");
            Group row = new SimpleGroupFactory(parquetSchema).newGroup().append("id", 1);
            row.addGroup("info").append("old_city", "Pune").append("zip", 411001);
            appendParquetFile(table, parquetSchema, row);

            Session session = pushdownSession(pushdown);
            assertQuery(session, "SELECT id, info.city, info.zip FROM " + tableName, "VALUES (1, 'Pune', 411001)");
            assertQuery(session, "SELECT info.city FROM " + tableName, "VALUES 'Pune'");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * The group has field IDs and a physical column that is literally named "city", but that column
     * carries a different field ID (77). The requested ID (3) is absent from the group, so the field
     * must read as NULL rather than being matched by name.
     */
    @Test(dataProvider = "pushdown")
    public void testMismatchedFieldIdDoesNotFallBackToName(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_mismatched_id_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Table table = createInfoTable(catalog, tableId);
            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id = 1;" +
                            " optional group info = 2 {" +
                            "  optional binary city (UTF8) = 77;" +
                            "  optional int32 zip = 4;" +
                            " }" +
                            "}");
            Group row = new SimpleGroupFactory(parquetSchema).newGroup().append("id", 1);
            row.addGroup("info").append("city", "WRONG").append("zip", 411001);
            appendParquetFile(table, parquetSchema, row);

            Session session = pushdownSession(pushdown);
            assertNullColumn(session, "SELECT info.city FROM " + tableName);
            assertNullColumn(session, "SELECT info.city, info.zip FROM " + tableName);
            assertQuery(session, "SELECT info.zip FROM " + tableName, "VALUES 411001");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * Only some children of the group carry field IDs. The group counts as ID-bearing, so the child
     * without an ID ("city") is not matched by name, with or without dereference pushdown.
     */
    @Test(dataProvider = "pushdown")
    public void testPartialFieldIdsDoNotFallBackToName(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_partial_ids_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Table table = createInfoTable(catalog, tableId);
            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id = 1;" +
                            " optional group info = 2 {" +
                            "  optional binary city (UTF8);" +
                            "  optional int32 zip = 4;" +
                            " }" +
                            "}");
            Group row = new SimpleGroupFactory(parquetSchema).newGroup().append("id", 1);
            row.addGroup("info").append("city", "WRONG").append("zip", 411001);
            appendParquetFile(table, parquetSchema, row);

            Session session = pushdownSession(pushdown);
            assertNullColumn(session, "SELECT info.city FROM " + tableName);
            assertNullColumn(session, "SELECT info.city, info.zip FROM " + tableName);
            assertQuery(session, "SELECT info.zip FROM " + tableName, "VALUES 411001");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * Mirror of the previous test: the requested field ("zip") is the one without an ID while its
     * sibling ("city") has one. It must read as NULL, not be matched by its name, and the sibling
     * that does have an ID must still be readable.
     */
    @Test(dataProvider = "pushdown")
    public void testPartialFieldIdsRequestedFieldWithoutId(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_partial_ids_requested_no_id_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Table table = createInfoTable(catalog, tableId);
            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id = 1;" +
                            " optional group info = 2 {" +
                            "  optional binary city (UTF8) = 3;" +
                            "  optional int32 zip;" +
                            " }" +
                            "}");
            Group row = new SimpleGroupFactory(parquetSchema).newGroup().append("id", 1);
            row.addGroup("info").append("city", "Pune").append("zip", 999999);
            appendParquetFile(table, parquetSchema, row);

            Session session = pushdownSession(pushdown);
            assertQuery(session, "SELECT info.city FROM " + tableName, "VALUES 'Pune'");
            assertNullColumn(session, "SELECT info.zip FROM " + tableName);
            assertNullColumn(session, "SELECT info.zip, info.city FROM " + tableName);
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * The rule is applied one group at a time. "info" carries field IDs (and a stale physical name
     * for "geo"), so it is resolved by ID; the nested "geo" group has no IDs, so "lat" is resolved
     * by name.
     */
    @Test(dataProvider = "pushdown")
    public void testIdBearingParentWithIdLessChildGroup(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_mixed_levels_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Schema schema = new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "info", Types.StructType.of(
                            Types.NestedField.optional(3, "city", Types.StringType.get()),
                            Types.NestedField.optional(4, "geo", Types.StructType.of(
                                    Types.NestedField.optional(5, "lat", Types.IntegerType.get()),
                                    Types.NestedField.optional(6, "lon", Types.IntegerType.get()))))));
            Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), ImmutableMap.of());

            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id = 1;" +
                            " optional group info = 2 {" +
                            "  optional binary city (UTF8) = 3;" +
                            "  optional group old_geo = 4 {" +
                            "   optional int32 lat;" +
                            "   optional int32 lon;" +
                            "  }" +
                            " }" +
                            "}");
            Group row = new SimpleGroupFactory(parquetSchema).newGroup().append("id", 1);
            Group info = row.addGroup("info");
            info.append("city", "Pune");
            info.addGroup("old_geo").append("lat", 18).append("lon", 73);
            appendParquetFile(table, parquetSchema, row);

            Session session = pushdownSession(pushdown);
            assertQuery(session, "SELECT info.geo.lat FROM " + tableName, "VALUES 18");
            assertQuery(session, "SELECT info.geo.lon, info.city FROM " + tableName, "VALUES (73, 'Pune')");
            assertQuery(session, "SELECT info.geo.lat, info.geo.lon FROM " + tableName, "VALUES (18, 73)");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * Level-by-level field resolution in the table schema allows resolving unrelated fields (e.g. "zip")
     * even when sibling fields differ only by case ("city" vs "CITY").
     */
    @Test(dataProvider = "pushdown")
    public void testCaseCollisionsInSchemaDoNotBreakUnrelatedFieldResolution(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_case_collisions_schema_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            Schema schema = new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "info", Types.StructType.of(
                            Types.NestedField.optional(3, "city", Types.StringType.get()),
                            Types.NestedField.optional(4, "CITY", Types.StringType.get()),
                            Types.NestedField.optional(5, "zip", Types.IntegerType.get()))));
            Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), ImmutableMap.of());
            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id = 1;" +
                            " optional group info = 2 {" +
                            "  optional binary city (UTF8) = 3;" +
                            "  optional int32 zip = 5;" +
                            " }" +
                            "}");
            Group row = new SimpleGroupFactory(parquetSchema).newGroup().append("id", 1);
            row.addGroup("info").append("city", "lower").append("zip", 411001);
            appendParquetFile(table, parquetSchema, row);

            Session session = pushdownSession(pushdown);
            assertQuery(session, "SELECT info.zip FROM " + tableName, "VALUES 411001");
            assertQuery(session, "SELECT info.zip, id FROM " + tableName, "VALUES (411001, 1)");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * Intermediate ROW projection (dereference pushdown of a struct subfield) must use the
     * Iceberg-ID-aware converter (IcebergParquetColumnIOConverter) for the pushed-down group, not
     * the shared name-based ColumnIOConverter.
     *
     * When the group carries a field renamed at the physical level (physical name "old_lat", logical
     * name "lat"), selecting info.geo.lat and info.geo.lon individually must return correct values
     * from the historical file. The whole-struct projection of info is also verified, which exercises
     * the non-pushdown IcebergParquetColumnIOConverter path and confirms that both renamed and
     * non-renamed children within the intermediate struct read correctly by field ID.
     */
    @Test(dataProvider = "pushdown")
    public void testIntermediateStructProjectionUsesIdAwareConverter(boolean pushdown)
            throws Exception
    {
        String tableName = "test_nested_intermediate_struct_id_" + pushdown;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(SCHEMA_NAME, tableName);
        try {
            // info.geo is an intermediate struct. info.geo.lat has been renamed from physical "old_lat".
            Schema schema = new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "info", Types.StructType.of(
                            Types.NestedField.optional(3, "city", Types.StringType.get()),
                            Types.NestedField.optional(4, "geo", Types.StructType.of(
                                    Types.NestedField.optional(5, "lat", Types.IntegerType.get()),
                                    Types.NestedField.optional(6, "lon", Types.IntegerType.get()))))));
            Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), ImmutableMap.of());

            // Physical file: geo.lat is stored as "old_lat" (simulating a historical file written
            // before the rename). The field ID 5 is present so ID-based matching must find it.
            MessageType parquetSchema = MessageTypeParser.parseMessageType(
                    "message table {" +
                            " optional int32 id = 1;" +
                            " optional group info = 2 {" +
                            "  optional binary city (UTF8) = 3;" +
                            "  optional group geo = 4 {" +
                            "   optional int32 old_lat = 5;" +
                            "   optional int32 lon = 6;" +
                            "  }" +
                            " }" +
                            "}");
            Group row = new SimpleGroupFactory(parquetSchema).newGroup().append("id", 1);
            Group info = row.addGroup("info");
            info.append("city", "Pune");
            info.addGroup("geo").append("old_lat", 18).append("lon", 73);
            appendParquetFile(table, parquetSchema, row);

            Session session = pushdownSession(pushdown);
            assertQuery(session, "SELECT info.geo.lat FROM " + tableName, "VALUES 18");
            assertQuery(session, "SELECT info.geo.lon FROM " + tableName, "VALUES 73");
            assertQuery(session, "SELECT info.geo.lat, info.geo.lon FROM " + tableName, "VALUES (18, 73)");
            // Whole-struct read of info exercises IcebergParquetColumnIOConverter recursively.
            // The renamed child (old_lat → lat) inside geo must read correctly in both modes.
            assertQuery(session, "SELECT info.city, info.geo.lat FROM " + tableName, "VALUES ('Pune', 18)");
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    private void assertNullColumn(Session session, String sql)
    {
        MaterializedResult result = computeActual(session, sql);
        assertEquals(result.getRowCount(), 1, sql);
        assertNull(result.getMaterializedRows().get(0).getField(0), sql);
    }

    private static Table createInfoTable(Catalog catalog, TableIdentifier tableId)
    {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "info", Types.StructType.of(
                        Types.NestedField.optional(3, "city", Types.StringType.get()),
                        Types.NestedField.optional(4, "zip", Types.IntegerType.get()))));
        return catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), ImmutableMap.of());
    }

    private Session pushdownSession(boolean enabled)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, "parquet_dereference_pushdown_enabled", String.valueOf(enabled))
                .build();
    }

    private static void appendParquetFile(Table table, MessageType parquetSchema, Group... rows)
            throws Exception
    {
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/test-file-" + UUID.randomUUID() + ".parquet");
        Configuration conf = new Configuration();
        // ExampleParquetWriter writes the schema exactly as given: Iceberg field IDs are present
        // only where the schema declares them.
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(filePath)
                .withConf(conf)
                .withType(parquetSchema)
                .build()) {
            for (Group row : rows) {
                writer.write(row);
            }
        }
        long length = filePath.getFileSystem(conf).getFileStatus(filePath).getLen();
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(filePath.toString())
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withFileSizeInBytes(length)
                .withRecordCount(rows.length)
                .build();
        table.newAppend().appendFile(dataFile).commit();
    }

    private Catalog loadCatalog()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        File catalogDirectory = getIcebergDataDirectoryPath(
                dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false).toFile();
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                ImmutableMap.of("warehouse", catalogDirectory.toURI().toString()), new Configuration());
    }
}
