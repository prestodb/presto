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

import com.facebook.presto.common.type.TypeManager;
import org.apache.iceberg.PartitionSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.iceberg.PartitionSpecConverter.toIcebergPartitionSpec;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.TestSchemaConverter.prestoIcebergSchema;
import static com.facebook.presto.iceberg.TestSchemaConverter.schema;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPartitionSpecConverter
{
    @DataProvider(name = "allTransforms")
    public static Object[][] testAllTransforms()
    {
        return new Object[][] {
                {"identity", "varchar"},
                {"year", "date"},
                {"month", "date"},
                {"day", "date"},
                {"bucket", "bigint"},
                {"truncate", "varchar"}
        };
    }

    @Test(dataProvider = "allTransforms")
    public void testToPrestoPartitionSpec(String transform, String name)
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Create a mock PartitionSpec
        PartitionSpec partitionSpec = partitionSpec(transform, name);

        PrestoIcebergPartitionSpec expectedPrestoPartitionSpec = prestoIcebergPartitionSpec(transform, name, typeManager);

        // Convert Iceberg PartitionSpec to Presto Iceberg Partition Spec
        PrestoIcebergPartitionSpec prestoIcebergPartitionSpec = toPrestoPartitionSpec(partitionSpec, typeManager);

        // Check that the result is not null
        assertNotNull(prestoIcebergPartitionSpec);

        assertEquals(prestoIcebergPartitionSpec, expectedPrestoPartitionSpec);
    }

    @Test(dataProvider = "allTransforms")
    public void testToIcebergPartitionSpec(String transform, String name)
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Create a mock PartitionSpec
        PrestoIcebergPartitionSpec prestoPartitionSpec = prestoIcebergPartitionSpec(transform, name, typeManager);

        PartitionSpec expectedPartitionSpec = partitionSpec(transform, name);

        // Convert Presto Partition Spec to Iceberg PartitionSpec
        PartitionSpec partitionSpec = toIcebergPartitionSpec(prestoPartitionSpec);

        // Check that the result is not null
        assertNotNull(partitionSpec);

        assertEquals(partitionSpec, expectedPartitionSpec);
    }

    @Test(dataProvider = "allTransforms")
    public void validateConversion(String transform, String name)
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Original Iceberg PartitionSpec
        PartitionSpec originalPartitionSpec = partitionSpec(transform, name);

        // Convert to Presto Partition Spec
        PrestoIcebergPartitionSpec prestoIcebergPartitionSpec = toPrestoPartitionSpec(originalPartitionSpec, typeManager);

        // Convert PrestoIcebergPartitionSpec back into Iceberg PartitionSpec
        PartitionSpec finalPartitionSpec = toIcebergPartitionSpec(prestoIcebergPartitionSpec);

        assertNotNull(finalPartitionSpec);

        assertEquals(originalPartitionSpec, finalPartitionSpec);

        assertTrue(originalPartitionSpec.schema().sameSchema(finalPartitionSpec.schema()));
    }

    private static PrestoIcebergPartitionSpec prestoIcebergPartitionSpec(String transform, String name, TypeManager typeManager)
    {
        List<String> fields = new ArrayList<>();

        switch (transform) {
            case "identity":
                fields.add(name);
                break;
            case "year":
            case "month":
            case "day":
                fields.add(format("%s(%s)", transform, name));
                break;
            case "bucket":
            case "truncate":
                fields.add(format("%s(%s, 3)", transform, name));
                break;
        }

        return new PrestoIcebergPartitionSpec(0, prestoIcebergSchema(typeManager), fields);
    }

    private static PartitionSpec partitionSpec(String transform, String name)
    {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema()).withSpecId(0);

        switch (transform) {
            case "identity":
                builder.identity(name);
                break;
            case "year":
                builder.year(name);
                break;
            case "month":
                builder.month(name);
                break;
            case "day":
                builder.day(name);
                break;
            case "bucket":
                builder.bucket(name, 3);
                break;
            case "truncate":
                builder.truncate(name, 3);
                break;
        }

        return builder.build();
    }
}
