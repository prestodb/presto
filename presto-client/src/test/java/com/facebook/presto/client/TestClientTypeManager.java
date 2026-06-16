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
package com.facebook.presto.client;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.HyperLogLogType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestClientTypeManager
{
    private ClientTypeManager typeManager;

    @BeforeMethod
    public void setUp()
    {
        typeManager = new ClientTypeManager();
    }

    @Test
    public void testPrimitiveTypes()
    {
        assertEquals(typeManager.getType(new TypeSignature("boolean")), BooleanType.BOOLEAN);
        assertEquals(typeManager.getType(new TypeSignature("tinyint")), TinyintType.TINYINT);
        assertEquals(typeManager.getType(new TypeSignature("smallint")), SmallintType.SMALLINT);
        assertEquals(typeManager.getType(new TypeSignature("integer")), IntegerType.INTEGER);
        assertEquals(typeManager.getType(new TypeSignature("bigint")), BigintType.BIGINT);
        assertEquals(typeManager.getType(new TypeSignature("real")), RealType.REAL);
        assertEquals(typeManager.getType(new TypeSignature("double")), DoubleType.DOUBLE);
        assertEquals(typeManager.getType(new TypeSignature("date")), DateType.DATE);
        assertEquals(typeManager.getType(new TypeSignature("time")), TimeType.TIME);
        assertEquals(typeManager.getType(new TypeSignature("time with time zone")), TimeWithTimeZoneType.TIME_WITH_TIME_ZONE);
        assertEquals(typeManager.getType(new TypeSignature("timestamp")), TimestampType.TIMESTAMP);
        assertEquals(typeManager.getType(new TypeSignature("timestamp with time zone")), TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(typeManager.getType(new TypeSignature("varbinary")), VarbinaryType.VARBINARY);
        assertEquals(typeManager.getType(new TypeSignature("json")), JsonType.JSON);
        assertEquals(typeManager.getType(new TypeSignature("uuid")), UuidType.UUID);
        assertEquals(typeManager.getType(new TypeSignature("HyperLogLog")), HyperLogLogType.HYPER_LOG_LOG);
    }

    @Test
    public void testVarcharType()
    {
        Type unboundedVarchar = typeManager.getType(new TypeSignature("varchar"));
        assertEquals(unboundedVarchar, VarcharType.VARCHAR);

        Type varchar10 = typeManager.getType(new TypeSignature(
                "varchar",
                ImmutableList.of(TypeSignatureParameter.of(10))));
        assertTrue(varchar10 instanceof VarcharType);
        assertEquals(varchar10, VarcharType.createVarcharType(10));

        Type varchar255 = typeManager.getType(new TypeSignature(
                "varchar",
                ImmutableList.of(TypeSignatureParameter.of(255))));
        assertTrue(varchar255 instanceof VarcharType);
        assertEquals(varchar255, VarcharType.createVarcharType(255));
    }

    @Test
    public void testCharType()
    {
        Type char1 = typeManager.getType(new TypeSignature("char"));
        assertTrue(char1 instanceof CharType);
        assertEquals(char1, CharType.createCharType(1));

        Type char10 = typeManager.getType(new TypeSignature(
                "char",
                ImmutableList.of(TypeSignatureParameter.of(10))));
        assertTrue(char10 instanceof CharType);
        assertEquals(char10, CharType.createCharType(10));

        Type char255 = typeManager.getType(new TypeSignature(
                "char",
                ImmutableList.of(TypeSignatureParameter.of(255))));
        assertTrue(char255 instanceof CharType);
        assertEquals(char255, CharType.createCharType(255));
    }

    @Test
    public void testDecimalType()
    {
        Type decimalType1 = typeManager.getType(new TypeSignature(
                "decimal",
                ImmutableList.of(
                        TypeSignatureParameter.of(10),
                        TypeSignatureParameter.of(2))));
        assertTrue(decimalType1 instanceof DecimalType);
        assertEquals(decimalType1, DecimalType.createDecimalType(10, 2));

        Type decimalType2 = typeManager.getType(new TypeSignature(
                "decimal",
                ImmutableList.of(
                        TypeSignatureParameter.of(38),
                        TypeSignatureParameter.of(10))));
        assertTrue(decimalType2 instanceof DecimalType);
        assertEquals(decimalType2, DecimalType.createDecimalType(38, 10));

        try {
            typeManager.getType(new TypeSignature(
                    "decimal",
                    ImmutableList.of(TypeSignatureParameter.of(10))));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("DECIMAL type must have exactly two parameters"));
        }
    }

    @Test
    public void testArrayTypes()
    {
        Type arrayOfBigint = typeManager.getType(new TypeSignature(
                "array",
                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint")))));
        assertTrue(arrayOfBigint instanceof ArrayType);
        ArrayType arrayType = (ArrayType) arrayOfBigint;
        assertEquals(arrayType.getElementType(), BigintType.BIGINT);

        Type arrayOfArrayOfInteger = typeManager.getType(new TypeSignature(
                "array",
                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature(
                        "array",
                        ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("integer"))))))));
        assertTrue(arrayOfArrayOfInteger instanceof ArrayType);
        ArrayType outerArray = (ArrayType) arrayOfArrayOfInteger;
        assertTrue(outerArray.getElementType() instanceof ArrayType);
        ArrayType innerArray = (ArrayType) outerArray.getElementType();
        assertEquals(innerArray.getElementType(), IntegerType.INTEGER);

        Type arrayOfVarchar = typeManager.getType(new TypeSignature(
                "array",
                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature(
                        "varchar",
                        ImmutableList.of(TypeSignatureParameter.of(100)))))));
        assertTrue(arrayOfVarchar instanceof ArrayType);
        ArrayType arrayOfVarcharType = (ArrayType) arrayOfVarchar;
        assertEquals(arrayOfVarcharType.getElementType(), VarcharType.createVarcharType(100));

        try {
            typeManager.getType(new TypeSignature(
                    "array",
                    ImmutableList.of(
                            TypeSignatureParameter.of(new TypeSignature("bigint")),
                            TypeSignatureParameter.of(new TypeSignature("varchar")))));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ARRAY type must have exactly one parameter"));
        }

        try {
            typeManager.getType(new TypeSignature(
                    "array",
                    ImmutableList.of(TypeSignatureParameter.of(42))));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ARRAY element must be a type"));
        }
    }

    @Test
    public void testMapTypes()
    {
        Type mapOfVarcharToBigint = typeManager.getType(new TypeSignature(
                "map",
                ImmutableList.of(
                        TypeSignatureParameter.of(new TypeSignature("varchar")),
                        TypeSignatureParameter.of(new TypeSignature("bigint")))));
        assertTrue(mapOfVarcharToBigint instanceof MapType);
        MapType mapType = (MapType) mapOfVarcharToBigint;
        assertEquals(mapType.getKeyType(), VarcharType.VARCHAR);
        assertEquals(mapType.getValueType(), BigintType.BIGINT);
        assertNotNull(mapType.getKeyBlockEquals());
        assertNotNull(mapType.getKeyBlockHashCode());

        Type mapOfVarcharToMapOfIntegerToDouble = typeManager.getType(new TypeSignature(
                "map",
                ImmutableList.of(
                        TypeSignatureParameter.of(new TypeSignature("varchar")),
                        TypeSignatureParameter.of(new TypeSignature(
                                "map",
                                ImmutableList.of(
                                        TypeSignatureParameter.of(new TypeSignature("integer")),
                                        TypeSignatureParameter.of(new TypeSignature("double"))))))));
        assertTrue(mapOfVarcharToMapOfIntegerToDouble instanceof MapType);
        MapType outerMap = (MapType) mapOfVarcharToMapOfIntegerToDouble;
        assertEquals(outerMap.getKeyType(), VarcharType.VARCHAR);
        assertTrue(outerMap.getValueType() instanceof MapType);
        MapType innerMap = (MapType) outerMap.getValueType();
        assertEquals(innerMap.getKeyType(), IntegerType.INTEGER);
        assertEquals(innerMap.getValueType(), DoubleType.DOUBLE);

        Type mapOfVarcharToArrayOfBigint = typeManager.getType(new TypeSignature(
                "map",
                ImmutableList.of(
                        TypeSignatureParameter.of(new TypeSignature("varchar")),
                        TypeSignatureParameter.of(new TypeSignature(
                                "array",
                                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint"))))))));
        assertTrue(mapOfVarcharToArrayOfBigint instanceof MapType);
        MapType mapWithArrayValue = (MapType) mapOfVarcharToArrayOfBigint;
        assertEquals(mapWithArrayValue.getKeyType(), VarcharType.VARCHAR);
        assertTrue(mapWithArrayValue.getValueType() instanceof ArrayType);
        ArrayType arrayType = (ArrayType) mapWithArrayValue.getValueType();
        assertEquals(arrayType.getElementType(), BigintType.BIGINT);

        try {
            typeManager.getType(new TypeSignature(
                    "map",
                    ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint")))));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("MAP type must have exactly two parameters"));
        }

        try {
            typeManager.getType(new TypeSignature(
                    "map",
                    ImmutableList.of(
                            TypeSignatureParameter.of(42),
                            TypeSignatureParameter.of(new TypeSignature("bigint")))));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("MAP key must be a type"));
        }
    }

    @Test
    public void testRowTypes()
    {
        Type row = typeManager.getType(new TypeSignature(
                "row",
                ImmutableList.of(
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.of(new RowFieldName("field1", false)),
                                new TypeSignature("bigint"))),
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.of(new RowFieldName("field2", false)),
                                new TypeSignature("varchar"))))));
        assertTrue(row instanceof RowType);
        RowType rowType = (RowType) row;
        List<RowType.Field> fields = rowType.getFields();
        assertEquals(fields.size(), 2);
        assertEquals(fields.get(0).getName(), Optional.of("field1"));
        assertEquals(fields.get(0).getType(), BigintType.BIGINT);
        assertEquals(fields.get(1).getName(), Optional.of("field2"));
        assertEquals(fields.get(1).getType(), VarcharType.VARCHAR);

        Type anonymousRow = typeManager.getType(new TypeSignature(
                "row",
                ImmutableList.of(
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature("bigint"))),
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature("varchar"))))));
        assertTrue(anonymousRow instanceof RowType);
        RowType anonymousRowType = (RowType) anonymousRow;
        List<RowType.Field> anonymousFields = anonymousRowType.getFields();
        assertEquals(anonymousFields.size(), 2);
        assertEquals(anonymousFields.get(0).getName(), Optional.empty());
        assertEquals(anonymousFields.get(0).getType(), BigintType.BIGINT);
        assertEquals(anonymousFields.get(1).getName(), Optional.empty());
        assertEquals(anonymousFields.get(1).getType(), VarcharType.VARCHAR);

        Type delimitedRow = typeManager.getType(new TypeSignature(
                "row",
                ImmutableList.of(
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.of(new RowFieldName("field with spaces", true)),
                                new TypeSignature("bigint"))))));
        assertTrue(delimitedRow instanceof RowType);
        RowType delimitedRowType = (RowType) delimitedRow;
        List<RowType.Field> delimitedFields = delimitedRowType.getFields();
        assertEquals(delimitedFields.size(), 1);
        assertEquals(delimitedFields.get(0).getName(), Optional.of("field with spaces"));
        assertTrue(delimitedFields.get(0).isDelimited());
        assertEquals(delimitedFields.get(0).getType(), BigintType.BIGINT);

        Type nestedRow = typeManager.getType(new TypeSignature(
                "row",
                ImmutableList.of(
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.of(new RowFieldName("outer", false)),
                                new TypeSignature(
                                        "row",
                                        ImmutableList.of(
                                                TypeSignatureParameter.of(new NamedTypeSignature(
                                                        Optional.of(new RowFieldName("inner", false)),
                                                        new TypeSignature("bigint"))))))))));
        assertTrue(nestedRow instanceof RowType);
        RowType outerRow = (RowType) nestedRow;
        assertEquals(outerRow.getFields().size(), 1);
        assertEquals(outerRow.getFields().get(0).getName(), Optional.of("outer"));
        assertTrue(outerRow.getFields().get(0).getType() instanceof RowType);
        RowType innerRow = (RowType) outerRow.getFields().get(0).getType();
        assertEquals(innerRow.getFields().size(), 1);
        assertEquals(innerRow.getFields().get(0).getName(), Optional.of("inner"));
        assertEquals(innerRow.getFields().get(0).getType(), BigintType.BIGINT);

        Type complexRow = typeManager.getType(new TypeSignature(
                "row",
                ImmutableList.of(
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.of(new RowFieldName("arr", false)),
                                new TypeSignature(
                                        "array",
                                        ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint")))))),
                        TypeSignatureParameter.of(new NamedTypeSignature(
                                Optional.of(new RowFieldName("map", false)),
                                new TypeSignature(
                                        "map",
                                        ImmutableList.of(
                                                TypeSignatureParameter.of(new TypeSignature("varchar")),
                                                TypeSignatureParameter.of(new TypeSignature("integer")))))))));
        assertTrue(complexRow instanceof RowType);
        RowType complexRowType = (RowType) complexRow;
        assertEquals(complexRowType.getFields().size(), 2);
        assertTrue(complexRowType.getFields().get(0).getType() instanceof ArrayType);
        assertTrue(complexRowType.getFields().get(1).getType() instanceof MapType);

        try {
            typeManager.getType(new TypeSignature("row", ImmutableList.of()));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ROW type must have at least one field"));
        }

        try {
            typeManager.getType(new TypeSignature(
                    "row",
                    ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint")))));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ROW parameters must be named types"));
        }
    }

    @Test
    public void testComplexType()
    {
        Type complexType = typeManager.getType(new TypeSignature(
                "array",
                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature(
                        "map",
                        ImmutableList.of(
                                TypeSignatureParameter.of(new TypeSignature("varchar")),
                                TypeSignatureParameter.of(new TypeSignature(
                                        "row",
                                        ImmutableList.of(
                                                TypeSignatureParameter.of(new NamedTypeSignature(
                                                        Optional.of(new RowFieldName("id", false)),
                                                        new TypeSignature("bigint"))),
                                                TypeSignatureParameter.of(new NamedTypeSignature(
                                                        Optional.of(new RowFieldName("values", false)),
                                                        new TypeSignature(
                                                                "array",
                                                                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("double")))))))))))))));
        assertTrue(complexType instanceof ArrayType);
        ArrayType arrayType = (ArrayType) complexType;
        assertTrue(arrayType.getElementType() instanceof MapType);
        MapType mapType = (MapType) arrayType.getElementType();
        assertEquals(mapType.getKeyType(), VarcharType.VARCHAR);
        assertTrue(mapType.getValueType() instanceof RowType);
        RowType rowType = (RowType) mapType.getValueType();
        assertEquals(rowType.getFields().size(), 2);
        assertEquals(rowType.getFields().get(0).getType(), BigintType.BIGINT);
        assertTrue(rowType.getFields().get(1).getType() instanceof ArrayType);
        ArrayType innerArray = (ArrayType) rowType.getFields().get(1).getType();
        assertEquals(innerArray.getElementType(), DoubleType.DOUBLE);
    }

    @Test
    public void testTypeCaching()
    {
        TypeSignature bigintSignature = new TypeSignature("bigint");
        Type type1 = typeManager.getType(bigintSignature);
        Type type2 = typeManager.getType(bigintSignature);
        assertSame(type1, type2);

        TypeSignature arraySignature = new TypeSignature(
                "array",
                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint"))));
        Type arrayType1 = typeManager.getType(arraySignature);
        Type arrayType2 = typeManager.getType(arraySignature);
        assertSame(arrayType1, arrayType2);
    }

    @Test
    public void testHasType()
    {
        assertTrue(typeManager.hasType(new TypeSignature("bigint")));
        assertTrue(typeManager.hasType(new TypeSignature(
                "array",
                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint"))))));
        assertTrue(typeManager.hasType(new TypeSignature(
                "map",
                ImmutableList.of(
                        TypeSignatureParameter.of(new TypeSignature("varchar")),
                        TypeSignatureParameter.of(new TypeSignature("bigint"))))));
    }

    @Test
    public void testGetParameterizedType()
    {
        Type arrayType = typeManager.getParameterizedType(
                "array",
                ImmutableList.of(TypeSignatureParameter.of(new TypeSignature("bigint"))));
        assertTrue(arrayType instanceof ArrayType);
        assertEquals(((ArrayType) arrayType).getElementType(), BigintType.BIGINT);
    }

    @Test
    public void testUnknownTypeThrowsException()
    {
        try {
            typeManager.getType(new TypeSignature("unknown_type"));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unknown type"));
        }
    }
}
