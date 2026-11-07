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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTypeConverter
{
    private static final TypeManager TYPE_MANAGER = createTestFunctionAndTypeManager();

    @DataProvider(name = "primitiveTypes")
    public static Object[][] primitiveTypes()
    {
        return new Object[][] {
                {"boolean", BooleanType.BOOLEAN},
                {"int8", TinyintType.TINYINT},
                {"int16", SmallintType.SMALLINT},
                {"int32", IntegerType.INTEGER},
                {"int64", BigintType.BIGINT},
                {"uint8", SmallintType.SMALLINT},
                {"uint16", IntegerType.INTEGER},
                {"uint32", BigintType.BIGINT},
                {"float32", RealType.REAL},
                {"float64", DoubleType.DOUBLE},
                {"date", DateType.DATE},
                {"time", TimeType.TIME},
                {"timestamp", TimestampType.TIMESTAMP},
                {"timestamp_s", TimestampType.TIMESTAMP},
                {"timestamp_ms", TimestampType.TIMESTAMP},
                {"timestamp_ns", TimestampType.TIMESTAMP},
                {"timestamptz", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE},
                {"varchar", VarcharType.createUnboundedVarcharType()},
                {"json", JsonType.JSON},
                {"uuid", UuidType.UUID},
                {"blob", VarbinaryType.VARBINARY},
        };
    }

    @Test(dataProvider = "primitiveTypes")
    public void testPrimitiveTypes(String duckLakeType, Type expected)
    {
        assertEquals(TypeConverter.toPrestoType(duckLakeType, "col"), expected);
    }

    @Test
    public void testDecimalWithoutSpace()
    {
        assertEquals(TypeConverter.toPrestoType("decimal(18,3)", "col"), DecimalType.createDecimalType(18, 3));
    }

    @Test
    public void testDecimalWithSpace()
    {
        assertEquals(TypeConverter.toPrestoType("decimal(18, 3)", "col"), DecimalType.createDecimalType(18, 3));
    }

    @DataProvider(name = "unsupportedTypes")
    public static Object[][] unsupportedTypes()
    {
        return new Object[][] {
                {"uint64"},
                {"int128"},
                {"uint128"},
                {"timetz"},
                {"interval"},
                {"variant"},
                {"geometry"},
                {"bogus"},
        };
    }

    @Test(dataProvider = "unsupportedTypes")
    public void testUnsupportedTypes(String duckLakeType)
    {
        PrestoException exception = expectThrows(PrestoException.class, () -> TypeConverter.toPrestoType(duckLakeType, "interval_col"));
        assertEquals(exception.getErrorCode(), DUCKLAKE_UNSUPPORTED_TYPE.toErrorCode());
        assertTrue(exception.getMessage().contains("interval_col"), exception.getMessage());
        assertTrue(exception.getMessage().contains(duckLakeType), exception.getMessage());
    }

    @Test
    public void testNestedTree()
    {
        // types.nested: id INTEGER, list_col ARRAY(INTEGER), struct_col ROW(a INTEGER, b VARCHAR),
        // map_col MAP(VARCHAR, INTEGER), struct_with_list ROW(name VARCHAR, tags ARRAY(VARCHAR))
        DuckLakeColumnIdentity id = primitive(1, "id", "int32");
        DuckLakeColumnIdentity listCol = new DuckLakeColumnIdentity(2, "list_col", TypeCategory.ARRAY, "list",
                ImmutableList.of(primitive(3, "element", "int32")));
        DuckLakeColumnIdentity structCol = new DuckLakeColumnIdentity(4, "struct_col", TypeCategory.STRUCT, "struct",
                ImmutableList.of(primitive(5, "a", "int32"), primitive(6, "b", "varchar")));
        DuckLakeColumnIdentity mapCol = new DuckLakeColumnIdentity(7, "map_col", TypeCategory.MAP, "map",
                ImmutableList.of(primitive(8, "key", "varchar"), primitive(9, "value", "int32")));
        DuckLakeColumnIdentity structWithList = new DuckLakeColumnIdentity(10, "struct_with_list", TypeCategory.STRUCT, "struct",
                ImmutableList.of(
                        primitive(11, "name", "varchar"),
                        new DuckLakeColumnIdentity(12, "tags", TypeCategory.ARRAY, "list",
                                ImmutableList.of(primitive(13, "element", "varchar")))));

        assertEquals(TypeConverter.toPrestoType(id, TYPE_MANAGER).getTypeSignature(), IntegerType.INTEGER.getTypeSignature());
        assertEquals(TypeConverter.toPrestoType(listCol, TYPE_MANAGER).getTypeSignature(), new ArrayType(IntegerType.INTEGER).getTypeSignature());
        assertEquals(
                TypeConverter.toPrestoType(structCol, TYPE_MANAGER).getTypeSignature().toString(),
                "row(a integer,b varchar)");
        assertEquals(
                TypeConverter.toPrestoType(mapCol, TYPE_MANAGER).getTypeSignature().toString(),
                "map(varchar,integer)");
        assertEquals(
                TypeConverter.toPrestoType(structWithList, TYPE_MANAGER).getTypeSignature().toString(),
                "row(name varchar,tags array(varchar))");
    }

    @Test
    public void testListOfStructOfMap()
    {
        DuckLakeColumnIdentity map = new DuckLakeColumnIdentity(3, "map_field", TypeCategory.MAP, "map",
                ImmutableList.of(primitive(4, "key", "varchar"), primitive(5, "value", "int64")));
        DuckLakeColumnIdentity struct = new DuckLakeColumnIdentity(2, "element", TypeCategory.STRUCT, "struct",
                ImmutableList.of(map));
        DuckLakeColumnIdentity list = new DuckLakeColumnIdentity(1, "outer_list", TypeCategory.ARRAY, "list",
                ImmutableList.of(struct));

        Type actual = TypeConverter.toPrestoType(list, TYPE_MANAGER);
        assertTrue(actual instanceof ArrayType, actual.toString());
        Type elementType = ((ArrayType) actual).getElementType();
        assertEquals(elementType.getTypeSignature().toString(), "row(map_field map(varchar,bigint))");
    }

    @Test
    public void testListWithTwoChildrenIsInvalid()
    {
        DuckLakeColumnIdentity list = new DuckLakeColumnIdentity(1, "bad_list", TypeCategory.ARRAY, "list",
                ImmutableList.of(primitive(2, "element", "int32"), primitive(3, "extra", "int32")));

        PrestoException exception = expectThrows(PrestoException.class, () -> TypeConverter.toPrestoType(list, TYPE_MANAGER));
        assertEquals(exception.getErrorCode(), DUCKLAKE_INVALID_METADATA.toErrorCode());
    }

    private static DuckLakeColumnIdentity primitive(long id, String name, String duckLakeType)
    {
        return new DuckLakeColumnIdentity(id, name, TypeCategory.PRIMITIVE, duckLakeType, ImmutableList.of());
    }
}
