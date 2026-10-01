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
package com.facebook.presto.hive.functions.schema;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSchemaMerger
{
    private static final TypeManager TYPE_MANAGER = FUNCTION_AND_TYPE_MANAGER;
    private static final String FILE_A = "file:///a.parquet";
    private static final String FILE_B = "file:///b.parquet";
    private static final String FILE_C = "file:///c.parquet";

    private static ColumnMetadata col(String name, Type type, boolean nullable)
    {
        return ColumnMetadata.builder().setName(name).setType(type).setNullable(nullable).build();
    }

    @Test
    public void testSingleFilePassthrough()
    {
        List<ColumnMetadata> file = ImmutableList.of(col("id", BIGINT, false), col("name", VARCHAR, true));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(file), ImmutableList.of(FILE_A), TYPE_MANAGER);
        assertEquals(merged, file);
    }

    @Test
    public void testIntegerWidening()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("n", INTEGER, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("n", BIGINT, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged, ImmutableList.of(col("n", BIGINT, false)));
    }

    @Test
    public void testIntegerChainTinyintToBigint()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("n", TINYINT, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("n", SMALLINT, false));
        List<ColumnMetadata> fileC = ImmutableList.of(col("n", BIGINT, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(
                ImmutableList.of(fileA, fileB, fileC),
                ImmutableList.of(FILE_A, FILE_B, FILE_C),
                TYPE_MANAGER);
        assertEquals(merged.get(0).getType(), BIGINT);
    }

    @Test
    public void testIntegerToDouble()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("v", INTEGER, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("v", DOUBLE, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.get(0).getType(), DOUBLE);
    }

    @Test
    public void testRealToDouble()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("v", REAL, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("v", DOUBLE, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.get(0).getType(), DOUBLE);
    }

    @Test
    public void testDecimalReprecision()
    {
        DecimalType a = DecimalType.createDecimalType(10, 2);
        DecimalType b = DecimalType.createDecimalType(12, 4);
        List<ColumnMetadata> fileA = ImmutableList.of(col("d", a, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("d", b, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        // integer digits: max(10-2, 12-4) = 8, scale = max(2, 4) = 4 → DECIMAL(12, 4)
        assertEquals(merged.get(0).getType(), DecimalType.createDecimalType(12, 4));
    }

    @Test
    public void testDecimalReprecisionOverflows()
    {
        DecimalType a = DecimalType.createDecimalType(38, 0);
        DecimalType b = DecimalType.createDecimalType(38, 10);
        List<ColumnMetadata> fileA = ImmutableList.of(col("d", a, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("d", b, false));
        try {
            SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
            fail("Expected PrestoException on decimal precision overflow");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertTrue(e.getMessage().contains("'d'"), e.getMessage());
            assertTrue(e.getMessage().contains(FILE_A) && e.getMessage().contains(FILE_B), e.getMessage());
        }
    }

    @Test
    public void testVarcharBoundedWidens()
    {
        VarcharType a = VarcharType.createVarcharType(10);
        VarcharType b = VarcharType.createVarcharType(20);
        List<ColumnMetadata> fileA = ImmutableList.of(col("s", a, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("s", b, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.get(0).getType(), VarcharType.createVarcharType(20));
    }

    @Test
    public void testVarcharUnboundedPrevails()
    {
        VarcharType a = VarcharType.createVarcharType(10);
        VarcharType b = VarcharType.createUnboundedVarcharType();
        List<ColumnMetadata> fileA = ImmutableList.of(col("s", a, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("s", b, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.get(0).getType(), VarcharType.createUnboundedVarcharType());
    }

    @Test
    public void testCharWidens()
    {
        CharType a = CharType.createCharType(5);
        CharType b = CharType.createCharType(8);
        List<ColumnMetadata> fileA = ImmutableList.of(col("c", a, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("c", b, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.get(0).getType(), CharType.createCharType(8));
    }

    @Test
    public void testCaseInsensitiveColumnKey()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("Name", VARCHAR, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("NAME", VARCHAR, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.size(), 1);
        assertEquals(merged.get(0).getName(), "Name");
    }

    @Test
    public void testPresenceMakesNullable()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("id", BIGINT, false), col("name", VARCHAR, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("id", BIGINT, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.size(), 2);
        assertEquals(merged.get(0).getName(), "id");
        assertFalse(merged.get(0).isNullable());
        assertEquals(merged.get(1).getName(), "name");
        assertTrue(merged.get(1).isNullable());
    }

    @Test
    public void testIncomingNullablePropagates()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("id", BIGINT, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("id", BIGINT, true));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertTrue(merged.get(0).isNullable());
    }

    @Test
    public void testOrderingFromFirstFileNewColumnsAppend()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("first", BIGINT, false), col("second", BIGINT, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("third", BIGINT, false), col("first", BIGINT, false));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.size(), 3);
        assertEquals(merged.get(0).getName(), "first");
        assertEquals(merged.get(1).getName(), "second");
        assertEquals(merged.get(2).getName(), "third");
    }

    @Test
    public void testIncompatibleTypeFails()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("x", BIGINT, false));
        List<ColumnMetadata> fileB = ImmutableList.of(col("x", VARCHAR, false));
        try {
            SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
            fail("Expected PrestoException on type conflict");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertTrue(e.getMessage().contains("'x'"), e.getMessage());
            assertTrue(e.getMessage().contains(FILE_A) && e.getMessage().contains(FILE_B), e.getMessage());
        }
    }

    @Test
    public void testRowRecursive()
    {
        Type rowA = RowType.from(ImmutableList.of(RowType.field("a", INTEGER), RowType.field("b", VARCHAR)));
        Type rowB = RowType.from(ImmutableList.of(RowType.field("a", BIGINT), RowType.field("c", VARCHAR)));
        List<ColumnMetadata> fileA = ImmutableList.of(col("r", rowA, true));
        List<ColumnMetadata> fileB = ImmutableList.of(col("r", rowB, true));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        Type expected = RowType.from(ImmutableList.of(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR),
                RowType.field("c", VARCHAR)));
        assertEquals(merged.get(0).getType(), expected);
    }

    @Test
    public void testArrayRecursive()
    {
        List<ColumnMetadata> fileA = ImmutableList.of(col("arr", new ArrayType(INTEGER), true));
        List<ColumnMetadata> fileB = ImmutableList.of(col("arr", new ArrayType(BIGINT), true));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        assertEquals(merged.get(0).getType(), new ArrayType(BIGINT));
    }

    @Test
    public void testMapRecursive()
    {
        Type mapA = TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(TypeSignatureParameter.of(VARCHAR.getTypeSignature()), TypeSignatureParameter.of(INTEGER.getTypeSignature())));
        Type mapB = TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(TypeSignatureParameter.of(VARCHAR.getTypeSignature()), TypeSignatureParameter.of(BIGINT.getTypeSignature())));
        List<ColumnMetadata> fileA = ImmutableList.of(col("m", mapA, true));
        List<ColumnMetadata> fileB = ImmutableList.of(col("m", mapB, true));
        List<ColumnMetadata> merged = SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
        Type expected = TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(TypeSignatureParameter.of(VARCHAR.getTypeSignature()), TypeSignatureParameter.of(BIGINT.getTypeSignature())));
        assertEquals(merged.get(0).getType(), expected);
    }

    @Test
    public void testNestedConflictMessageNamesPath()
    {
        Type rowA = RowType.from(ImmutableList.of(RowType.field("inner", BIGINT)));
        Type rowB = RowType.from(ImmutableList.of(RowType.field("inner", VARCHAR)));
        List<ColumnMetadata> fileA = ImmutableList.of(col("outer", rowA, true));
        List<ColumnMetadata> fileB = ImmutableList.of(col("outer", rowB, true));
        try {
            SchemaMerger.merge(ImmutableList.of(fileA, fileB), ImmutableList.of(FILE_A, FILE_B), TYPE_MANAGER);
            fail("Expected PrestoException on nested type conflict");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertTrue(e.getMessage().contains("outer.inner"), e.getMessage());
        }
    }

    @Test
    public void testEmptyFilesFails()
    {
        try {
            SchemaMerger.merge(ImmutableList.of(), ImmutableList.of(), TYPE_MANAGER);
            fail("Expected PrestoException on empty input");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
        }
    }
}
