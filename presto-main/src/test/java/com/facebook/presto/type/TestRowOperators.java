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
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

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
        assertFunction("cast(cast (null as ROW(BIGINT, VARCHAR)) AS JSON)", JSON, null);
        assertFunction("cast(ROW(null, null) as json)", JSON, "[null,null]");

        assertFunction("cast(ROW(true, false, null) AS JSON)", JSON, "[true,false,null]");

        assertFunction(
                "cast(cast(ROW(12, 12345, 123456789, 1234567890123456789, null, null, null, null) AS ROW(TINYINT, SMALLINT, INTEGER, BIGINT, TINYINT, SMALLINT, INTEGER, BIGINT)) AS JSON)",
                JSON,
                "[12,12345,123456789,1234567890123456789,null,null,null,null]");

        assertFunction(
                "CAST(ROW(CAST(3.14 AS REAL), 3.1415, 1e308, DECIMAL '3.14', DECIMAL '12345678901234567890.123456789012345678', CAST(null AS REAL), CAST(null AS DOUBLE), CAST(null AS DECIMAL)) AS JSON)",
                JSON,
                "[3.14,3.1415,1.0E308,3.14,12345678901234567890.123456789012345678,null,null,null]"
        );

        assertFunction(
                "CAST(ROW('a', 'bb', CAST(null as VARCHAR), JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', CAST(null AS JSON)) AS JSON)",
                JSON,
                "[\"a\",\"bb\",null,123,3.14,false,\"abc\",[1,\"a\",null],{\"a\":1,\"b\":\"str\",\"c\":null},null,null]");
        assertFunction(
                "CAST(ROW(DATE '2001-08-22', DATE '2001-08-23', null) AS JSON)",
                JSON,
                "[\"2001-08-22\",\"2001-08-23\",null]");

        assertFunction(
                "CAST(ROW(from_unixtime(1), cast(null as TIMESTAMP)) AS JSON)",
                JSON,
                format("[\"%s\",null]", sqlTimestamp(1000).toString()));

        assertFunction(
                "cast(ROW(ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], CAST(null AS ARRAY<BIGINT>)) AS JSON)",
                JSON,
                "[[1,2],[3,null],[],[null,null],null]");
        assertFunction(
                "cast(ROW(MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), CAST(NULL as MAP<VARCHAR, BIGINT>)) AS JSON)",
                JSON,
                "[{\"a\":1,\"b\":2},{\"none\":null,\"three\":3},{},{\"h1\":null,\"h2\":null},null]");
        assertFunction(
                "cast(ROW(ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null) AS JSON)",
                JSON,
                "[[1,2],[3,null],[null,null],null]");

        // other miscellaneous tests
        assertFunction("CAST(ROW(1, 2) AS JSON)", JSON, "[1,2]");
        assertFunction("CAST(CAST(ROW(1, 2) AS ROW(a BIGINT, b BIGINT)) AS JSON)", JSON, "[1,2]");
        assertFunction("CAST(ROW(1, NULL) AS JSON)", JSON, "[1,null]");
        assertFunction("CAST(ROW(1, CAST(NULL AS INTEGER)) AS JSON)", JSON, "[1,null]");
        assertFunction("CAST(ROW(1, 2.0) AS JSON)", JSON, "[1,2.0]");
        assertFunction("CAST(ROW(1.0, 2.5) AS JSON)", JSON, "[1.0,2.5]");
        assertFunction("CAST(ROW(1.0, 'kittens') AS JSON)", JSON, "[1.0,\"kittens\"]");
        assertFunction("CAST(ROW(TRUE, FALSE) AS JSON)", JSON, "[true,false]");
        assertFunction("CAST(ROW(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS JSON)", JSON, "[false,[1,2],{\"1\":2.0,\"3\":4.0}]");
    }

    @Test
    public void testFieldAccessor()
            throws Exception
    {
        assertFunction("CAST(row(1, CAST(NULL AS DOUBLE)) AS ROW(col0 integer, col1 double)).col1", DOUBLE, null);
        assertFunction("CAST(row(TRUE, CAST(NULL AS BOOLEAN)) AS ROW(col0 boolean, col1 boolean)).col1", BOOLEAN, null);
        assertFunction("CAST(row(TRUE, CAST(NULL AS ARRAY<INTEGER>)) AS ROW(col0 boolean, col1 array(integer))).col1", new ArrayType(INTEGER), null);
        assertFunction("CAST(row(1.0, CAST(NULL AS VARCHAR)) AS ROW(col0 double, col1 varchar)).col1", createUnboundedVarcharType(), null);
        assertFunction("CAST(row(1, 2) AS ROW(col0 integer, col1 integer)).col0", INTEGER, 1);
        assertFunction("CAST(row(1, 'kittens') AS ROW(col0 integer, col1 varchar)).col1", createUnboundedVarcharType(), "kittens");
        assertFunction("CAST(row(1, 2) AS ROW(col0 integer, col1 integer)).\"col1\"", INTEGER, 2);
        assertFunction("CAST(array[row(1, 2)] AS array(row(col0 integer, col1 integer)))[1].col1", INTEGER, 2);
        assertFunction("CAST(row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS ROW(col0 boolean , col1 array(integer), col2 map(integer, double))).col1", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("CAST(row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS ROW(col0 boolean , col1 array(integer), col2 map(integer, double))).col2", new MapType(INTEGER, DOUBLE), ImmutableMap.of(1, 2.0, 3, 4.0));
        assertFunction("CAST(row(1.0, ARRAY[row(31, 4.1), row(32, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))).col1[2].col0", INTEGER, 32);

        // Using ROW constructor
        assertFunction("CAST(ROW(1, 2) AS ROW(a BIGINT, b DOUBLE)).a", BIGINT, 1L);
        assertFunction("CAST(ROW(1, 2) AS ROW(a BIGINT, b DOUBLE)).b", DOUBLE, 2.0);
        assertFunction("CAST(ROW(CAST(ROW('aa') AS ROW(a VARCHAR))) AS ROW(a ROW(a VARCHAR))).a.a", createUnboundedVarcharType(), "aa");
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

        assertInvalidFunction("CAST(ROW(1, 2) AS ROW(a BIGINT, A DOUBLE)).a");

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
    public void testRowComparison()
            throws Exception
    {
        assertFunction("row(TIMESTAMP '2002-01-02 03:04:05.321 +08:10', TIMESTAMP '2002-01-02 03:04:05.321 +08:10') = " +
                "row(TIMESTAMP '2002-01-02 02:04:05.321 +07:10', TIMESTAMP '2002-01-02 03:05:05.321 +08:11')", BOOLEAN, true);
        assertFunction("row(1.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')) = " +
                "row(1.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11'))", BOOLEAN, false);

        assertComparisonCombination("row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')",
                "row(TIMESTAMP '2002-01-02 03:04:05.321 +07:09', TIMESTAMP '2002-01-02 03:04:05.321 +07:09')");
        assertComparisonCombination("row(1.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))",
                "row(2.0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))");

        assertComparisonCombination("row(1.0, 'kittens')", "row(1.0, 'puppies')");
        assertComparisonCombination("row(1, 2.0)", "row(5, 2.0)");
        assertComparisonCombination("row(TRUE, FALSE, TRUE, FALSE)", "row(TRUE, TRUE, TRUE, FALSE)");
        assertComparisonCombination("row(1, 2.0, TRUE, 'kittens', from_unixtime(1))", "row(1, 3.0, TRUE, 'kittens', from_unixtime(1))");

        assertInvalidFunction("cast(row(cast(cast ('' as varbinary) as hyperloglog)) as row(col0 hyperloglog)) = cast(row(cast(cast ('' as varbinary) as hyperloglog)) as row(col0 hyperloglog))",
                SemanticErrorCode.TYPE_MISMATCH, "line 1:81: '=' cannot be applied to row(col0 HyperLogLog), row(col0 HyperLogLog)");
        assertInvalidFunction("cast(row(cast(cast ('' as varbinary) as hyperloglog)) as row(col0 hyperloglog)) > cast(row(cast(cast ('' as varbinary) as hyperloglog)) as row(col0 hyperloglog))",
                SemanticErrorCode.TYPE_MISMATCH, "line 1:81: '>' cannot be applied to row(col0 HyperLogLog), row(col0 HyperLogLog)");

        assertFunction("row(TRUE, ARRAY [1], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) = row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))", BOOLEAN, false);
        assertFunction("row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) = row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))", BOOLEAN, true);

        assertInvalidFunction("row(1, CAST(NULL AS INTEGER)) = row(1, 2)", StandardErrorCode.NOT_SUPPORTED);
        assertInvalidFunction("row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) > row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0]))",
                SemanticErrorCode.TYPE_MISMATCH, "line 1:60: '>' cannot be applied to row(field0 boolean,field1 array(integer),field2 map(integer,double)), row(field0 boolean,field1 array(integer),field2 map(integer,double))");

        assertInvalidFunction("row(1, CAST(NULL AS INTEGER)) < row(1, 2)", StandardErrorCode.NOT_SUPPORTED);

        assertComparisonCombination("row(1.0, ARRAY [1,2,3], row(2,2.0))", "row(1.0, ARRAY [1,3,3], row(2,2.0))");
        assertComparisonCombination("row(TRUE, ARRAY [1])", "row(TRUE, ARRAY [1, 2])");
        assertComparisonCombination("ROW(1, 2)", "ROW(2, 1)");
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

    private void assertComparisonCombination(String base, String greater)
    {
        Set<String> equalOperators = new HashSet<>(ImmutableSet.of("=", ">=", "<="));
        Set<String> greaterOrInequalityOperators = new HashSet<>(ImmutableSet.of(">=", ">", "!="));
        Set<String> lessOrInequalityOperators = new HashSet<>(ImmutableSet.of("<=", "<", "!="));
        for (String operator : ImmutableList.of(">", "=", "<", ">=", "<=", "!=")) {
            assertFunction(base + operator + base, BOOLEAN, equalOperators.contains(operator));
            assertFunction(base + operator + greater, BOOLEAN, lessOrInequalityOperators.contains(operator));
            assertFunction(greater + operator + base, BOOLEAN, greaterOrInequalityOperators.contains(operator));
        }
    }

    private static SqlTimestamp sqlTimestamp(long millisUtc)
    {
        return new SqlTimestamp(millisUtc, TEST_SESSION.getTimeZoneKey());
    }
}
