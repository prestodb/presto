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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.getLocationProvider;
import static org.apache.iceberg.LocationProviders.locationsFor;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestIcebergWriteLocation
{
    private static final String TABLE_LOCATION = "ws://warehouse/table-guid/registration-guid/7";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get();

    @Test
    public void testDefaultIsTheTableLocation()
    {
        Table table = table(ImmutableMap.of());
        IcebergWriteLocation location = IcebergWriteLocation.forTable(table);

        assertEquals(location.getLocation(), TABLE_LOCATION);
        assertEquals(location.getStorageProperties(), table.properties());
        assertTrue(location.isIncludePartitionPaths());
        assertProviderPaths(table.locationProvider(), location);
    }

    @Test
    public void testDefaultRetainsFilenameHashing()
    {
        Table table = table(ImmutableMap.of(
                OBJECT_STORE_ENABLED, "true",
                WRITE_DATA_LOCATION, "ws://warehouse/external-data"));
        IcebergWriteLocation location = IcebergWriteLocation.forTable(table);

        assertEquals(location.getLocation(), TABLE_LOCATION);
        assertEquals(location.getStorageProperties(), table.properties());
        assertProviderPaths(table.locationProvider(), location);
    }

    @Test
    public void testDirectoryProviderUsesTheProviderDirectory()
    {
        LocationProvider currentProvider = locationsFor(TABLE_LOCATION,
                ImmutableMap.of(WRITE_DATA_LOCATION, "ws://warehouse/table-guid/registration-guid/8/"));
        Table table = table(ImmutableMap.of(WRITE_DATA_LOCATION, TABLE_LOCATION + "/old-data"), currentProvider);

        IcebergWriteLocation location = IcebergWriteLocation.forDirectoryProvider(table, currentProvider, true);

        assertEquals(location.getLocation(), "ws://warehouse/table-guid/registration-guid/8");
        assertProviderPaths(currentProvider, location);
        assertEquals(table.properties().get(WRITE_DATA_LOCATION), TABLE_LOCATION + "/old-data");
    }

    @Test
    public void testDirectoryProviderRejectsPathWithoutDirectory()
    {
        LocationProvider provider = new LocationProvider()
        {
            @Override
            public String newDataLocation(String filename)
            {
                return filename;
            }

            @Override
            public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename)
            {
                return filename;
            }
        };

        assertThrows(IllegalStateException.class, () -> IcebergWriteLocation.forDirectoryProvider(table(ImmutableMap.of()), provider, false));
    }

    @Test
    public void testFlatDirectoryIgnoresPhysicalPartitionPaths()
    {
        IcebergWriteLocation location = new IcebergWriteLocation(TABLE_LOCATION + "/", ImmutableMap.of(), false);
        LocationProvider provider = workerProvider(location);
        org.apache.iceberg.PartitionData partition = partition();

        assertEquals(provider.newDataLocation("file.parquet"), TABLE_LOCATION + "/file.parquet");
        assertEquals(provider.newDataLocation(SPEC, partition, "file.parquet"), TABLE_LOCATION + "/file.parquet");
        assertFalse(location.isIncludePartitionPaths());
    }

    @Test(dataProvider = "partitionLayouts")
    public void testOutputHandleJsonRoundTrip(boolean includePartitionPaths)
            throws Exception
    {
        ObjectNode json = (ObjectNode) OBJECT_MAPPER.valueToTree(outputHandle(includePartitionPaths));
        IcebergOutputTableHandle copy = OBJECT_MAPPER.treeToValue(json, IcebergOutputTableHandle.class);

        assertEquals(copy.getOutputPath(), TABLE_LOCATION);
        assertEquals(copy.isIncludePartitionPaths(), includePartitionPaths);
    }

    // A coordinator that predates includePartitionPaths omits it; the handle must keep the legacy layout.
    @Test
    public void testOutputHandleWithoutIncludePartitionPathsKeepsPartitionPaths()
            throws Exception
    {
        ObjectNode json = (ObjectNode) OBJECT_MAPPER.valueToTree(outputHandle(false));

        json.remove("includePartitionPaths");
        assertTrue(OBJECT_MAPPER.treeToValue(json, IcebergOutputTableHandle.class).isIncludePartitionPaths());
        json.putNull("includePartitionPaths");
        assertTrue(OBJECT_MAPPER.treeToValue(json, IcebergOutputTableHandle.class).isIncludePartitionPaths());
    }

    @DataProvider
    public Object[][] partitionLayouts()
    {
        return new Object[][] {{true}, {false}};
    }

    private static IcebergOutputTableHandle outputHandle(boolean includePartitionPaths)
    {
        PrestoIcebergSchema schema = new PrestoIcebergSchema(1, ImmutableList.of(), ImmutableMap.of(), null, ImmutableSet.of());
        return new IcebergOutputTableHandle(
                "schema",
                new IcebergTableName("table", DATA, Optional.empty(), Optional.empty(), Optional.empty()),
                schema,
                new PrestoIcebergPartitionSpec(0, schema, ImmutableList.of()),
                ImmutableList.of(),
                TABLE_LOCATION,
                includePartitionPaths,
                PARQUET,
                HiveCompressionCodec.NONE,
                ImmutableMap.of(),
                ImmutableList.of());
    }

    private static Table table(Map<String, String> properties)
    {
        return table(properties, null);
    }

    // A real table over fixed metadata; currentProvider overrides the provider derived from it.
    private static Table table(Map<String, String> properties, LocationProvider currentProvider)
    {
        TableMetadata metadata = TableMetadata.newTableMetadata(SCHEMA, SPEC, TABLE_LOCATION, properties);
        LocationProvider provider = currentProvider != null ? currentProvider : locationsFor(TABLE_LOCATION, metadata.properties());
        return new BaseTable(new FixedTableOperations(metadata, provider), "table");
    }

    private static final class FixedTableOperations
            implements TableOperations
    {
        private final TableMetadata metadata;
        private final LocationProvider locationProvider;

        private FixedTableOperations(TableMetadata metadata, LocationProvider locationProvider)
        {
            this.metadata = metadata;
            this.locationProvider = locationProvider;
        }

        @Override
        public TableMetadata current()
        {
            return metadata;
        }

        @Override
        public TableMetadata refresh()
        {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata newMetadata)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIO io()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public EncryptionManager encryption()
        {
            return PlaintextEncryptionManager.instance();
        }

        @Override
        public String metadataFileLocation(String fileName)
        {
            return TABLE_LOCATION + "/metadata/" + fileName;
        }

        @Override
        public LocationProvider locationProvider()
        {
            return locationProvider;
        }
    }

    private static LocationProvider workerProvider(IcebergWriteLocation location)
    {
        return getLocationProvider(new SchemaTableName("schema", "table"),
                location.getLocation(), location.getStorageProperties(), location.isIncludePartitionPaths());
    }

    private static org.apache.iceberg.PartitionData partition()
    {
        org.apache.iceberg.PartitionData partition = new org.apache.iceberg.PartitionData(SPEC.partitionType());
        partition.set(0, 42);
        return partition;
    }

    private static void assertProviderPaths(LocationProvider expected, IcebergWriteLocation location)
    {
        LocationProvider actual = workerProvider(location);
        assertEquals(actual.newDataLocation("file.parquet"), expected.newDataLocation("file.parquet"));
        assertEquals(actual.newDataLocation(SPEC, partition(), "file.parquet"), expected.newDataLocation(SPEC, partition(), "file.parquet"));
    }
}
