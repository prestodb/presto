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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestRowOperators
        extends AbstractTestFunctions
{
    public TestRowOperators() {}

    @Test
    public void testRowTypeLookup()
            throws Exception
    {
        functionAssertions.getMetadata().getType(parseTypeSignature("row(a bigint)"));
        Type type = functionAssertions.getMetadata().getType(parseTypeSignature("row(b bigint)"));
        assertEquals(type.getTypeSignature().getParameters().size(), 1);
        assertEquals(type.getTypeSignature().getParameters().get(0).getNamedTypeSignature().getName(), "b");
    }

    @Test
    public void testRowToJson()
            throws Exception
    {
        assertFunction("CAST(ROW(1, 2) AS JSON)", JSON, "[1,2]");
        assertFunction("CAST(CAST(ROW(1, 2) AS ROW(a BIGINT, b BIGINT)) AS JSON)", JSON, "[1,2]");
        assertFunction("CAST(ROW(1, NULL) AS JSON)", JSON, "[1,null]");
        assertFunction("CAST(ROW(1, CAST(NULL AS INTEGER)) AS JSON)", JSON, "[1,null]");
        assertFunction("CAST(ROW(1, 2.0) AS JSON)", JSON, "[1,2.0]");
        assertFunction("CAST(ROW(1.0, 2.5) AS JSON)", JSON, "[1.0,2.5]");
        assertFunction("CAST(ROW(1.0, 'kittens') AS JSON)", JSON, "[1.0,\"kittens\"]");
        assertFunction("CAST(ROW(TRUE, FALSE) AS JSON)", JSON, "[true,false]");
        assertFunction("CAST(ROW(from_unixtime(1)) AS JSON)", JSON, "[\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()) + "\"]");
        assertFunction("CAST(ROW(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS JSON)", JSON, "[false,[1,2],{\"1\":2.0,\"3\":4.0}]");
    }

    @Test
    public void testFieldAccessor()
            throws Exception
    {
        assertFunction("CAST(row(1, CAST(NULL AS DOUBLE)) AS ROW(col0 integer, col1 double)).col1", DOUBLE, null);
        assertFunction("CAST(row(TRUE, CAST(NULL AS BOOLEAN)) AS ROW(col0 boolean, col1 boolean)).col1", BOOLEAN, null);
        assertFunction("CAST(row(TRUE, CAST(NULL AS ARRAY<INTEGER>)) AS ROW(col0 boolean, col1 array(integer))).col1", new ArrayType(INTEGER), null);
        assertFunction("CAST(row(1.0, CAST(NULL AS VARCHAR)) AS ROW(col0 double, col1 varchar)).col1", VARCHAR, null);
        assertFunction("CAST(row(1, 2) AS ROW(col0 integer, col1 integer)).col0", INTEGER, 1);
        assertFunction("CAST(row(1, 'kittens') AS ROW(col0 integer, col1 varchar)).col1", VARCHAR, "kittens");
        assertFunction("CAST(row(1, 2) AS ROW(col0 integer, col1 integer)).\"col1\"", INTEGER, 2);
        assertFunction("CAST(array[row(1, 2)] AS array(row(col0 integer, col1 integer)))[1].col1", INTEGER, 2);
        assertFunction("CAST(row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS ROW(col0 boolean , col1 array(integer), col2 map(integer, double))).col1", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("CAST(row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS ROW(col0 boolean , col1 array(integer), col2 map(integer, double))).col2", new MapType(INTEGER, DOUBLE), ImmutableMap.of(1, 2.0, 3, 4.0));
        assertFunction("CAST(row(1.0, ARRAY[row(31, 4.1), row(32, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))).col1[2].col0", INTEGER, 32);

        // Using ROW constructor
        assertFunction("CAST(ROW(1, 2) AS ROW(a BIGINT, b DOUBLE)).a", BIGINT, 1L);
        assertFunction("CAST(ROW(1, 2) AS ROW(a BIGINT, b DOUBLE)).b", DOUBLE, 2.0);
        assertFunction("CAST(ROW(CAST(ROW('aa') AS ROW(a VARCHAR))) AS ROW(a ROW(a VARCHAR))).a.a", VARCHAR, "aa");
        assertFunction("CAST(ROW(ROW('ab')) AS ROW(a ROW(b VARCHAR))).a.b", VARCHAR, "ab");
        assertFunction("CAST(ROW(ARRAY[NULL]) AS ROW(a ARRAY(BIGINT))).a", new ArrayType(BIGINT), Arrays.asList((Integer) null));

        // Row type is not case sensitive
        assertFunction("CAST(ROW(1) AS ROW(A BIGINT)).A", BIGINT, 1L);
    }

    @Test
    public void testRowCast()
    {
        assertFunction("cast(row(2, 3) as row(aa bigint, bb bigint)).aa", BIGINT, 2L);
        assertFunction("cast(row(2, 3) as row(aa bigint, bb bigint)).bb", BIGINT, 3L);
        assertFunction("cast(row(2, 3) as row(aa bigint, bb boolean)).bb", BOOLEAN, true);
        assertFunction("cast(row(2, cast(null as double)) as row(aa bigint, bb double)).bb", DOUBLE, null);
        assertFunction("cast(row(2, 'test_str') as row(aa bigint, bb varchar)).bb", VARCHAR, "test_str");

        // ROW casting with NULLs
        assertFunction("cast(row(1,null,3) as row(aa bigint, bb boolean, cc boolean)).bb", BOOLEAN, null);
        assertFunction("cast(row(1,null,3) as row(aa bigint, bb boolean, cc boolean)).aa", BIGINT, 1L);
        assertFunction("cast(row(null,null,null) as row(aa bigint, bb boolean, cc boolean)).aa", BIGINT, null);

        try {
            assertFunction("CAST(ROW(1, 2) AS ROW(a BIGINT, A DOUBLE)).a", BIGINT, 1L);
            fail("fields in Row are case insensitive");
        }
        catch (RuntimeException e) {
            // Expected
        }

        // there are totally 7 field names
        String longFieldNameCast = "CAST(row(1.2, ARRAY[row(233, 6.9)], row(1000, 6.3)) AS ROW(%s VARCHAR, %s ARRAY(ROW(%s VARCHAR, %s VARCHAR)), %s ROW(%s VARCHAR, %s VARCHAR))).%s[1].%s";
        int fieldCount = 7;
        char[] chars = new char[9333];
        String[] fields = new String[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            Arrays.fill(chars, (char) ('a' + i));
            fields[i] = new String(chars);
        }
        assertFunction(format(longFieldNameCast, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[1], fields[2]), VARCHAR, "233");
    }

    @Test
    public void testIsDistinctFrom()
            throws Exception
    {
        assertFunction("CAST(NULL AS ROW(UNKNOWN)) IS DISTINCT FROM CAST(NULL AS ROW(UNKNOWN))", BOOLEAN, false);
        assertFunction("row(NULL) IS DISTINCT FROM row(NULL)", BOOLEAN, false);
        assertFunction("row(1, 'cat') IS DISTINCT FROM row(1, 'cat')", BOOLEAN, false);
        assertFunction("row(1, ARRAY [1]) IS DISTINCT FROM row(1, ARRAY [1])", BOOLEAN, false);
        assertFunction("row(1, ARRAY [1, 2]) IS DISTINCT FROM row(1, ARRAY [1, NULL])", BOOLEAN, true);
        assertFunction("row(1, 2.0, TRUE, 'cat', from_unixtime(1)) IS DISTINCT FROM row(1, 2.0, TRUE, 'cat', from_unixtime(1))", BOOLEAN, false);
        assertFunction("row(1, 2.0, TRUE, 'cat', from_unixtime(1)) IS DISTINCT FROM row(1, 2.0, TRUE, 'cat', from_unixtime(2))", BOOLEAN, true);
        assertFunction("row(1, 2.0, TRUE, 'cat', CAST(NULL AS INTEGER)) IS DISTINCT FROM row(1, 2.0, TRUE, 'cat', 2)", BOOLEAN, true);
        assertFunction("row(1, 2.0, TRUE, 'cat', CAST(NULL AS INTEGER)) IS DISTINCT FROM row(1, 2.0, TRUE, 'cat', CAST(NULL AS INTEGER))", BOOLEAN, false);
        assertFunction("row(1, 2.0, TRUE, 'cat') IS DISTINCT FROM row(1, 2.0, TRUE, CAST(NULL AS VARCHAR(3)))", BOOLEAN, true);
        assertFunction("row(1, 2.0, TRUE, CAST(NULL AS VARCHAR(3))) IS DISTINCT FROM row(1, 2.0, TRUE, CAST(NULL AS VARCHAR(3)))", BOOLEAN, false);
    }

    @Test
    public void testRowEquality()
            throws Exception
    {
        assertFunction("row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10') = " +
                "row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')", BOOLEAN, true);
        assertFunction("row(1.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')) =" +
                "row(1.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))", BOOLEAN, true);
        assertFunction("row(1.0, 'kittens') = row(1.0, 'kittens')", BOOLEAN, true);
        assertFunction("row(1, 2.0) = row(1, 2.0)", BOOLEAN, true);
        assertFunction("row(TRUE, FALSE, TRUE, FALSE) = row(TRUE, FALSE, TRUE, FALSE)", BOOLEAN, true);
        assertFunction("row(TRUE, FALSE, TRUE, FALSE) = row(TRUE, TRUE, TRUE, FALSE)", BOOLEAN, false);
        assertFunction("row(1, 2.0, TRUE, 'kittens', from_unixtime(1)) = row(1, 2.0, TRUE, 'kittens', from_unixtime(1))", BOOLEAN, true);

        assertFunction("row(1.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')) !=" +
                "row(1.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11'))", BOOLEAN, true);
        assertFunction("row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10') != " +
                "row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11')", BOOLEAN, true);
        assertFunction("row(1.0, 'kittens') != row(1.0, 'kittens')", BOOLEAN, false);
        assertFunction("row(1, 2.0) != row(1, 2.0)", BOOLEAN, false);
        assertFunction("row(TRUE, FALSE, TRUE, FALSE) != row(TRUE, FALSE, TRUE, FALSE)", BOOLEAN, false);
        assertFunction("row(TRUE, FALSE, TRUE, FALSE) != row(TRUE, TRUE, TRUE, FALSE)", BOOLEAN, true);
        assertFunction("row(1, 2.0, TRUE, 'kittens', from_unixtime(1)) != row(1, 2.0, TRUE, 'puppies', from_unixtime(1))", BOOLEAN, true);

        try {
            assertFunction("cast(row(cast(cast ('' as varbinary) as hyperloglog)) as row(col0 hyperloglog)) = cast(row(cast(cast ('' as varbinary) as hyperloglog)) as row(col0 hyperloglog))", BOOLEAN, true);
            fail("hyperloglog is not comparable");
        }
        catch (SemanticException e) {
            if (!e.getMessage().matches("\\Qline 1:81: '=' cannot be applied to row(COL0 HyperLogLog), row(COL0 HyperLogLog)\\E")) {
                throw e;
            }
            //Expected
        }

        assertFunction("row(TRUE, ARRAY [1], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) = row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))", BOOLEAN, false);
        assertFunction("row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) = row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))", BOOLEAN, true);

        try {
            assertFunction("row(1, CAST(NULL AS INTEGER)) = row(1, 2)", BOOLEAN, false);
            fail("ROW comparison not implemented for NULL values");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode().getCode(), StandardErrorCode.NOT_SUPPORTED.toErrorCode().getCode());
        }

        assertFunction("row(TRUE, ARRAY [1]) = row(TRUE, ARRAY [1])", BOOLEAN, true);
        assertFunction("row(TRUE, ARRAY [1]) = row(TRUE, ARRAY [1,2])", BOOLEAN, false);
        assertFunction("row(1.0, ARRAY [1,2,3], row(2,2.0)) = row(1.0, ARRAY [1,2,3], row(2,2.0))", BOOLEAN, true);

        assertFunction("row(TRUE, ARRAY [1]) != row(TRUE, ARRAY [1])", BOOLEAN, false);
        assertFunction("row(TRUE, ARRAY [1]) != row(TRUE, ARRAY [1,2])", BOOLEAN, true);
        assertFunction("row(1.0, ARRAY [1,2,3], row(2,2.0)) != row(1.0, ARRAY [1,2,3], row(1,2.0))", BOOLEAN, true);

        assertFunction("ROW(1, 2) = ROW(1, 2)", BOOLEAN, true);
        assertFunction("ROW(2, 1) != ROW(1, 2)", BOOLEAN, true);
    }

    @Test
    public void testRowHashOperator()
    {
        assertRowHashOperator("ROW(1, 2)", ImmutableList.of(INTEGER, INTEGER), ImmutableList.of(1, 2));
        assertRowHashOperator("ROW(true, 2)", ImmutableList.of(BOOLEAN, INTEGER), ImmutableList.of(true, 2));
    }

    private void assertRowHashOperator(String inputString, List<Type> types, List<Object> elements)
    {
        checkArgument(types.size() == elements.size(), "types and elements must have the same size");
        RowType rowType = new RowType(types, Optional.empty());
        BlockBuilder rowArrayBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder rowBuilder = new InterleavedBlockBuilder(types, new BlockBuilderStatus(), types.size());
        for (int i = 0; i < types.size(); i++) {
            appendToBlockBuilder(types.get(i), elements.get(i), rowBuilder);
        }
        rowType.writeObject(rowArrayBuilder, rowBuilder.build());

        assertOperator(HASH_CODE, inputString, BIGINT, rowType.hash(rowArrayBuilder.build(), 0));
    }
}
