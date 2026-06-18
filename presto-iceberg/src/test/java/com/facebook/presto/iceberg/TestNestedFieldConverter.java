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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.iceberg.NestedFieldConverter.toIcebergNestedField;
import static com.facebook.presto.iceberg.NestedFieldConverter.toPrestoNestedField;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestNestedFieldConverter
{
    @DataProvider(name = "allTypes")
    public static Object[][] testAllTypes()
    {
        return new Object[][] {
                {1, "boolean"},
                {1, "integer"},
                {1, "array"},
                {1, "bigint"},
                {1, "real"},
                {1, "double"},
                {1, "map"},
                {1, "decimal"},
                {1, "varchar"},
                {1, "varbinary"},
                {1, "row"},
                {1, "date"},
                {1, "nested"}
        };
    }

    @Test(dataProvider = "allTypes")
    public void testToPrestoNestedField(int id, String name)
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Create a mock NestedField
        Types.NestedField nestedField = nestedField(id, name);

        PrestoIcebergNestedField expectedPrestoNestedField = prestoIcebergNestedField(id, name, typeManager);

        // Convert Iceberg NestedField to Presto Nested Field
        PrestoIcebergNestedField prestoNestedField = toPrestoNestedField(nestedField, typeManager);

        // Check that the result is not null
        assertNotNull(prestoNestedField);

        assertEquals(prestoNestedField, expectedPrestoNestedField);
    }

    @Test(dataProvider = "allTypes")
    public void testToIcebergNestedField(int id, String name)
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Create a mock Presto Nested Field
        PrestoIcebergNestedField prestoNestedField = prestoIcebergNestedField(id, name, typeManager);

        Types.NestedField expectedNestedField = nestedField(id, name);

        // Convert Presto Nested Field to Iceberg NestedField
        Types.NestedField nestedField = toIcebergNestedField(prestoNestedField, columnNameToIdMapping(name));

        // Check that the result is not null
        assertNotNull(nestedField);

        assertEquals(nestedField, expectedNestedField);
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "Cannot convert Row type field row\\(integer, varchar\\) to Iceberg")
    public void testFailOnAnonymousRowType()
    {
        // Create a test TypeManager
        TypeManager typeManager = createTestFunctionAndTypeManager();

        // Create a mock Presto Nested Field
        PrestoIcebergNestedField prestoNestedField = prestoIcebergNestedField(1, "anonymous_row", typeManager);

        // Convert Presto Nested Field to Iceberg NestedField
        Types.NestedField nestedField = toIcebergNestedField(prestoNestedField, columnNameToIdMapping("anonymous_row"));
    }

    protected static PrestoIcebergNestedField prestoIcebergNestedField(
            int id,
            String name,
            TypeManager typeManager)
    {
        com.facebook.presto.common.type.Type prestoType;
        switch (name) {
            case "boolean":
                prestoType = BOOLEAN;
                break;
            case "integer":
                prestoType = INTEGER;
                break;
            case "array":
                prestoType = new ArrayType(createUnboundedVarcharType());
                break;
            case "bigint":
                prestoType = BIGINT;
                break;
            case "real":
                prestoType = REAL;
                break;
            case "double":
                prestoType = DOUBLE;
                break;
            case "map":
                prestoType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                        TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                        TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature())));
                break;
            case "decimal":
                prestoType = createDecimalType(5, 2);
                break;
            case "varchar":
                prestoType = createUnboundedVarcharType();
                break;
            case "varbinary":
                prestoType = VARBINARY;
                break;
            case "row":
                prestoType = RowType.from(ImmutableList.of(
                        RowType.field("int", INTEGER),
                        RowType.field("varchar", createUnboundedVarcharType())));
                break;
            case "anonymous_row":
                prestoType = RowType.from(ImmutableList.of(
                        RowType.field(INTEGER),
                        RowType.field(createUnboundedVarcharType())));
                break;
            case "date":
                prestoType = DATE;
                break;
            case "timestamp":
                prestoType = TIMESTAMP;
                break;
            case "timestamptz":
                prestoType = TIMESTAMP_WITH_TIME_ZONE;
                break;
            case "nested":
                prestoType = RowType.from(ImmutableList.of(
                        RowType.field("int", INTEGER),
                        RowType.field("varchar", createUnboundedVarcharType()),
                        RowType.field(
                                "row",
                                RowType.from(ImmutableList.of(
                                        RowType.field("int", INTEGER),
                                        RowType.field("varchar", createUnboundedVarcharType())))),
                        RowType.field(
                                "map",
                                typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                        TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                        TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature()))))));
                break;
            default:
                prestoType = null;
        }

        return new PrestoIcebergNestedField(true, id, name, prestoType, Optional.empty());
    }

    protected static Types.NestedField nestedField(int id, String name)
    {
        Type icebergType;

        switch (name) {
            case "boolean":
                icebergType = Types.BooleanType.get();
                break;
            case "integer":
                icebergType = Types.IntegerType.get();
                break;
            case "array":
                icebergType = array();
                break;
            case "bigint":
                icebergType = Types.LongType.get();
                break;
            case "real":
                icebergType = Types.FloatType.get();
                break;
            case "double":
                icebergType = Types.DoubleType.get();
                break;
            case "map":
                icebergType = map();
                break;
            case "decimal":
                icebergType = decimal(5, 2);
                break;
            case "varchar":
                icebergType = Types.StringType.get();
                break;
            case "varbinary":
                icebergType = Types.BinaryType.get();
                break;
            case "row":
                icebergType = struct();
                break;
            case "date":
                icebergType = Types.DateType.get();
                break;
            case "timestamp":
                icebergType = Types.TimestampType.withoutZone();
                break;
            case "timestamptz":
                icebergType = Types.TimestampType.withZone();
                break;
            case "nested":
                icebergType = nested();
                break;
            default:
                icebergType = null;
        }

        return optional(id, name, icebergType);
    }

    private Map<String, Integer> columnNameToIdMapping(String name)
    {
        Map<String, Integer> mapping = new HashMap<>();
        switch (name) {
            case "boolean":
            case "integer":
            case "bigint":
            case "real":
            case "double":
            case "decimal":
            case "varchar":
            case "varbinary":
            case "date":
                mapping.put(name, 1);
                break;
            case "array":
                mapping.put(name, 1);
                mapping.put(name + ".element", 1);
                break;
            case "map":
                mapping.put(name, 1);
                mapping.put(name + ".key", 1);
                mapping.put(name + ".value", 2);
                break;
            case "row":
                mapping.put(name, 1);
                mapping.put(name + ".int", 1);
                mapping.put(name + ".varchar", 2);
                break;
            case "nested":
                mapping.put(name, 1);
                mapping.put(name + ".int", 1);
                mapping.put(name + ".varchar", 2);
                mapping.put(name + ".row", 3);
                mapping.put(name + ".map", 4);
                mapping.put(name + ".row.int", 1);
                mapping.put(name + ".row.varchar", 2);
                mapping.put(name + ".map.key", 1);
                mapping.put(name + ".map.value", 2);
                break;
        }
        return mapping;
    }

    private static Type array()
    {
        return Types.ListType.ofOptional(1, Types.StringType.get());
    }

    private static Type map()
    {
        return Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get());
    }

    private static Type decimal(int precision, int scale)
    {
        return Types.DecimalType.of(precision, scale);
    }

    private static Type struct()
    {
        List<Types.NestedField> fields = new ArrayList<>();

        fields.add(optional(1, "int", Types.IntegerType.get()));
        fields.add(optional(2, "varchar", Types.StringType.get()));

        return Types.StructType.of(fields);
    }

    private static Type nested()
    {
        List<Types.NestedField> fields = new ArrayList<>();

        fields.add(optional(1, "int", Types.IntegerType.get()));
        fields.add(optional(2, "varchar", Types.StringType.get()));
        fields.add(optional(3, "row", struct()));
        fields.add(optional(4, "map", map()));

        return Types.StructType.of(fields);
    }
}
