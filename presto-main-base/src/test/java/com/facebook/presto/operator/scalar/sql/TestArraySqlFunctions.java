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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class TestArraySqlFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testArrayAverage()
    {
        assertFunctionWithError("array_average(array[1, 2])", DOUBLE, 1.5);
        assertFunctionWithError("array_average(array[1, bigint '2', smallint '3', tinyint '4', 5.0])", DOUBLE, 3.0);

        assertFunctionWithError("array_average(array[1, null, 2, null])", DOUBLE, 1.5);
        assertFunctionWithError("array_average(array[null, null, 1])", DOUBLE, 1.0);

        assertFunction("array_average(array[null])", DOUBLE, null);
        assertFunction("array_average(array[null, null])", DOUBLE, null);
        assertFunction("array_average(null)", DOUBLE, null);
    }

    @Test
    public void testArraySplitIntoChunksBigint()
    {
        assertFunction("array_split_into_chunks(array[bigint '1', bigint '2', bigint '3'], 2)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(3L)));
        assertFunction("array_split_into_chunks(array[bigint '1', bigint '2', bigint '3', bigint '4' , bigint '5'], 2)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(3L, 4L), ImmutableList.of(5L)));
        assertFunction("array_split_into_chunks(array[bigint '2', bigint '3', bigint '4' , bigint '5'], 4)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(2L, 3L, 4L, 5L)));
        assertFunction("array_split_into_chunks(array[bigint '-66', bigint '3', bigint '-66' , bigint '5'], 1)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(-66L), ImmutableList.of(3L), ImmutableList.of(-66L), ImmutableList.of(5L)));
        assertFunction("array_split_into_chunks(array[bigint '-1', bigint '2', bigint '3' , bigint '-11'], 6)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(-1L, 2L, 3L, -11L)));
        assertFunction("array_split_into_chunks(array[bigint '1', bigint '2', bigint '3', bigint '4', bigint '5', bigint '6', bigint '7'], 3)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(ImmutableList.of(1L, 2L, 3L), ImmutableList.of(4L, 5L, 6L), ImmutableList.of(7L)));
        assertInvalidFunction("array_split_into_chunks(array[bigint '-1', bigint '2', bigint '3' , bigint '-11'], 0)", StandardErrorCode.GENERIC_USER_ERROR, "Invalid slice size: 0. Size must be greater than zero.");
        assertInvalidFunction(
                "array_split_into_chunks(array[" + IntStream.rangeClosed(1, 12001).mapToObj(Long::toString).collect(Collectors.joining(", ")) + "], 1)",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Cannot split array of size: 12001 into more than 10000 parts.");
    }

    @Test
    public void testArraySplitIntoChunksVarchar()
    {
        assertFunction("array_split_into_chunks(array[varchar 'a', varchar 'b', varchar 'c'], 2)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c")));
        assertFunction("array_split_into_chunks(array[varchar 'a', varchar 'b', varchar 'c', varchar 'd', varchar 'e'], 2)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c", "d"), ImmutableList.of("e")));
        assertFunction("array_split_into_chunks(array[varchar 'z', varchar 'y', varchar 'x', varchar 'w', varchar 'v', varchar 'u'], 6)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("z", "y", "x", "w", "v", "u")));
        assertFunction("array_split_into_chunks(array[varchar 'k', varchar 'l', varchar 'm'], 1)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("k"), ImmutableList.of("l"), ImmutableList.of("m")));
        assertFunction("array_split_into_chunks(array[varchar 'k', varchar 'l', varchar 'm'], 8)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("k", "l", "m")));
        assertFunction("array_split_into_chunks(array[varchar 'k', varchar 'l', varchar 'm', varchar 'n', varchar 'o', varchar 'p', varchar 'q', varchar 'r'], 3)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(ImmutableList.of("k", "l", "m"), ImmutableList.of("n", "o", "p"), ImmutableList.of("q", "r")));
        assertInvalidFunction("array_split_into_chunks(array[varchar 'a', varchar 'b', varchar 'c', varchar 'd'], 0)", StandardErrorCode.GENERIC_USER_ERROR, "Invalid slice size: 0. Size must be greater than zero.");
        assertInvalidFunction(
                "array_split_into_chunks(array[" + IntStream.rangeClosed(1, 10002).mapToObj(s -> "'" + s + "'").collect(Collectors.joining(", ")) + "], 1)",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Cannot split array of size: 10002 into more than 10000 parts.");
    }

    @Test
    public void testArraySplitIntoChunksInteger()
    {
        assertFunction("array_split_into_chunks(array[1, 2, 3], 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));
        assertFunction("array_split_into_chunks(array[1, 2, 3], 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));
        assertFunction("array_split_into_chunks(array[1, 2, 5, 7, 9, 11], 4)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2, 5, 7), ImmutableList.of(9, 11)));
        assertFunction("array_split_into_chunks(array[1, 2, 5, 7, 9, 11, 22], 7)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2, 5, 7, 9, 11, 22)));
        assertFunction("array_split_into_chunks(array[9, 10, 11, 12], 1)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(9), ImmutableList.of(10), ImmutableList.of(11), ImmutableList.of(12)));
        assertFunction("array_split_into_chunks(array[9, 10, 11, 12], 20)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(9, 10, 11, 12)));
        assertFunction("array_split_into_chunks(array[9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 4)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(9, 10, 11, 12), ImmutableList.of(13, 14, 15, 16), ImmutableList.of(17, 18, 19, 20)));
        assertInvalidFunction("array_split_into_chunks(array[5, 6, 7, 8], 0)", StandardErrorCode.GENERIC_USER_ERROR, "Invalid slice size: 0. Size must be greater than zero.");
        assertInvalidFunction(
                "array_split_into_chunks(array[" + IntStream.rangeClosed(1, 10001).mapToObj(Integer::toString).collect(Collectors.joining(", ")) + "], 1)",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Cannot split array of size: 10001 into more than 10000 parts.");
    }

    @Test
    public void testArraySplitIntoChunksDouble()
    {
        assertFunction("array_split_into_chunks(array[cast(1.0 as double), cast(2.0 as double), cast(3.0 as double)], 2)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(ImmutableList.of(1.0, 2.0), ImmutableList.of(3.0)));
        assertFunction("array_split_into_chunks(array[cast(1.2 as double), cast(2.3 as double), cast(3.4 as double)], 1)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(ImmutableList.of(1.2), ImmutableList.of(2.3), ImmutableList.of(3.4)));
        assertFunction("array_split_into_chunks(array[cast(1.2 as double), cast(2.3 as double), cast(3.4 as double), cast(4.5 as double), cast(5.6 as double)], 5)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(ImmutableList.of(1.2, 2.3, 3.4, 4.5, 5.6)));
        assertFunction("array_split_into_chunks(array[cast(1.2 as double), cast(2.3 as double), cast(3.4 as double), cast(4.5 as double), cast(5.6 as double)], 100)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(ImmutableList.of(1.2, 2.3, 3.4, 4.5, 5.6)));
        assertFunction("array_split_into_chunks(array[cast(1.2 as double), cast(2.3 as double), cast(3.4 as double), cast(4.5 as double), cast(5.6 as double),  cast(6.7 as double), cast(7.8 as double), cast(8.9 as double), cast(9.1 as double)], 3)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(ImmutableList.of(1.2, 2.3, 3.4), ImmutableList.of(4.5, 5.6, 6.7), ImmutableList.of(7.8, 8.9, 9.1)));
        assertInvalidFunction("array_split_into_chunks(array[cast(1.2 as double), cast(2.3 as double), cast(3.4 as double)], 0)", StandardErrorCode.GENERIC_USER_ERROR, "Invalid slice size: 0. Size must be greater than zero.");
        assertInvalidFunction(
                "array_split_into_chunks(array[" + IntStream.rangeClosed(1, 10001).mapToObj(Double::toString).collect(Collectors.joining(", ")) + "], 1)",
                StandardErrorCode.GENERIC_USER_ERROR,
                "Cannot split array of size: 10001 into more than 10000 parts.");
    }

    @Test
    public void testArraySplitIntoChunksNulls()
    {
        assertFunction("array_split_into_chunks(array[cast(null as bigint), bigint '1', cast(null as bigint), bigint '2'], 2)", new ArrayType(new ArrayType(BIGINT)), ImmutableList.of(asList(null, 1L), asList(null, 2L)));
        assertFunction("array_split_into_chunks(array[cast(null as varchar), cast(null as varchar)], 2)", new ArrayType(new ArrayType(VARCHAR)), ImmutableList.of(asList(null, null)));
        assertFunction("array_split_into_chunks(array[cast(null as double), 1.1, 2.1, 3.1], 2)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(asList(null, 1.1), asList(2.1, 3.1)));
        assertFunction("array_split_into_chunks(array[1, 2, 3, cast(null as int)], 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1, 2), asList(3, null)));
        assertFunction("array_split_into_chunks(null, null)", new ArrayType(new ArrayType(UNKNOWN)), null);
        assertFunction("array_split_into_chunks(null, 1)", new ArrayType(new ArrayType(UNKNOWN)), null);
        assertFunction("array_split_into_chunks(array[1], null)", new ArrayType(new ArrayType(INTEGER)), null);
    }

    @Test
    public void testArrayFrequencyBigint()
    {
        assertFunction("array_frequency(cast(null as array(bigint)))", createMapType(BIGINT, INTEGER), null);
        assertFunction("array_frequency(cast(array[] as array(bigint)))", createMapType(BIGINT, INTEGER), ImmutableMap.of());
        assertFunction("array_frequency(array[cast(null as bigint), cast(null as bigint), cast(null as bigint)])", createMapType(BIGINT, INTEGER), ImmutableMap.of());
        assertFunction("array_frequency(array[cast(null as bigint), bigint '1'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(1L, 1));
        assertFunction("array_frequency(array[cast(null as bigint), bigint '1', bigint '3', cast(null as bigint), bigint '1', bigint '3', cast(null as bigint)])", createMapType(BIGINT, INTEGER), ImmutableMap.of(1L, 2, 3L, 2));
        assertFunction("array_frequency(array[bigint '1', bigint '1', bigint '2', bigint '2', bigint '3', bigint '1', bigint '3', bigint '2'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(1L, 3, 2L, 3, 3L, 2));
        assertFunction("array_frequency(array[bigint '45'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(45L, 1));
        assertFunction("array_frequency(array[bigint '-45'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(-45L, 1));
        assertFunction("array_frequency(array[bigint '1', bigint '3', bigint '1', bigint '3'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(1L, 2, 3L, 2));
        assertFunction("array_frequency(array[bigint '3', bigint '1', bigint '3',bigint '1'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(1L, 2, 3L, 2));
        assertFunction("array_frequency(array[bigint '4',bigint '3',bigint '3',bigint '2',bigint '2',bigint '2',bigint '1',bigint '1',bigint '1',bigint '1'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(1L, 4, 2L, 3, 3L, 2, 4L, 1));
        assertFunction("array_frequency(array[bigint '3', bigint '3', bigint '2', bigint '2', bigint '5', bigint '5', bigint '1', bigint '1'])", createMapType(BIGINT, INTEGER), ImmutableMap.of(1L, 2, 2L, 2, 3L, 2, 5L, 2));
    }

    @Test
    public void testArrayFrequencyVarchar()
    {
        assertFunction("array_frequency(cast(null as array(varchar)))", createMapType(VARCHAR, INTEGER), null);
        assertFunction("array_frequency(cast(array[] as array(varchar)))", createMapType(VARCHAR, INTEGER), ImmutableMap.of());
        assertFunction("array_frequency(array[cast(null as varchar), cast(null as varchar), cast(null as varchar)])", createMapType(VARCHAR, INTEGER), ImmutableMap.of());
        assertFunction("array_frequency(array[varchar 'z', cast(null as varchar)])", createMapType(VARCHAR, INTEGER), ImmutableMap.of("z", 1));
        assertFunction("array_frequency(array[varchar 'a', cast(null as varchar), varchar 'b', cast(null as varchar), cast(null as varchar) ])", createMapType(VARCHAR, INTEGER), ImmutableMap.of("a", 1, "b", 1));
        assertFunction("array_frequency(array[varchar 'a', varchar 'b', varchar 'a', varchar 'a', varchar 'a'])", createMapType(VARCHAR, INTEGER), ImmutableMap.of("a", 4, "b", 1));
        assertFunction("array_frequency(array[varchar 'a', varchar 'b', varchar 'a', varchar 'b', varchar 'c'])", createMapType(VARCHAR, INTEGER), ImmutableMap.of("a", 2, "b", 2, "c", 1));
        assertFunction("array_frequency(array[varchar 'y', varchar 'p'])", createMapType(VARCHAR, INTEGER), ImmutableMap.of("p", 1, "y", 1));
        assertFunction("array_frequency(array[varchar 'a', varchar 'a', varchar 'p'])", createMapType(VARCHAR, INTEGER), ImmutableMap.of("p", 1, "a", 2));
        assertFunction("array_frequency(array[varchar 'z'])", createMapType(VARCHAR, INTEGER), ImmutableMap.of("z", 1));
    }

    @Test
    public void testArrayFrequencyComplexTypes()
    {
        assertFunction("array_frequency(cast(null as array(array(varchar))))", createMapType(new ArrayType(VARCHAR), INTEGER), null);
        assertFunction("array_frequency(cast(array[] as array(array(varchar))))", createMapType(new ArrayType(VARCHAR), INTEGER), ImmutableMap.of());
        assertFunction("array_frequency(array[cast(null as array(varchar)), cast(null as array(varchar)), cast(null as array(varchar))])", createMapType(new ArrayType(VARCHAR), INTEGER), ImmutableMap.of());
        assertFunction("array_frequency(array[array[varchar 'z'], array[varchar 'z']])", createMapType(new ArrayType(VARCHAR), INTEGER), ImmutableMap.of(singletonList("z"), 2));
        assertFunction("array_frequency(array[array[varchar 'z'], array[varchar 't']])", createMapType(new ArrayType(VARCHAR), INTEGER), ImmutableMap.of(singletonList("z"), 1, singletonList("t"), 1));

        RowType rowType = RowType.from(ImmutableList.of(RowType.field(INTEGER), RowType.field(INTEGER)));
        String t = rowType.toString();
        assertFunction("array_frequency(array[(1, 2), (1, 3), (1, 2)])", createMapType(rowType, INTEGER), ImmutableMap.of(ImmutableList.of(1, 2), 2, ImmutableList.of(1, 3), 1));
        assertInvalidFunction("array_frequency(array[(1, null), (null, 2), (null, 1)])", StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
        assertInvalidFunction("array_frequency(array[(null, 1), (1, null), (null, null)])", StandardErrorCode.NOT_SUPPORTED, "map key cannot be null or contain nulls");
    }

    @Test
    public void testArrayHasDuplicates()
    {
        assertFunction("array_has_duplicates(cast(null as array(varchar)))", BOOLEAN, null);
        assertFunction("array_has_duplicates(cast(array[] as array(varchar)))", BOOLEAN, false);

        assertFunction("array_has_duplicates(array[varchar 'a', varchar 'b', varchar 'a'])", BOOLEAN, true);
        assertFunction("array_has_duplicates(array[varchar 'a', varchar 'b'])", BOOLEAN, false);
        assertFunction("array_has_duplicates(array[varchar 'a', varchar 'a'])", BOOLEAN, true);

        assertFunction("array_has_duplicates(array[1, 2, 1])", BOOLEAN, true);
        assertFunction("array_has_duplicates(array[1, 2])", BOOLEAN, false);
        assertFunction("array_has_duplicates(array[1, 1, 1])", BOOLEAN, true);

        assertFunction("array_has_duplicates(array[0, null])", BOOLEAN, false);
        assertFunction("array_has_duplicates(array[0, null, null])", BOOLEAN, true);

        assertFunction("array_has_duplicates(array[array[1], array[2], array[]])", BOOLEAN, false);
        assertFunction("array_has_duplicates(array[array[1], array[2], array[2]])", BOOLEAN, true);
        assertFunction("array_has_duplicates(array[(1, 2), (1, 2)])", BOOLEAN, true);
        assertFunction("array_has_duplicates(array[(1, 2), (2, 2)])", BOOLEAN, false);
        assertInvalidFunction("array_has_duplicates(array[(1, null), (null, 2), (null, 1)])", StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
        assertInvalidFunction("array_has_duplicates(array[(1, null), (null, 2), (null, null)])", StandardErrorCode.NOT_SUPPORTED, "map key cannot be null or contain nulls");
    }

    @Test
    public void testArrayDuplicates()
    {
        assertFunction("array_duplicates(cast(null as array(varchar)))", new ArrayType(VARCHAR), null);
        assertFunction("array_duplicates(cast(array[] as array(varchar)))", new ArrayType(VARCHAR), ImmutableList.of());

        assertFunction("array_duplicates(array[varchar 'a', varchar 'b', varchar 'a'])", new ArrayType(VARCHAR), ImmutableList.of("a"));
        assertFunction("array_duplicates(array[varchar 'a', varchar 'b'])", new ArrayType(VARCHAR), ImmutableList.of());
        assertFunction("array_duplicates(array[varchar 'a', varchar 'a'])", new ArrayType(VARCHAR), ImmutableList.of("a"));

        assertFunction("array_duplicates(array[1, 2, 1])", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("array_duplicates(array[1, 2])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_duplicates(array[1, 1, 1])", new ArrayType(INTEGER), ImmutableList.of(1));

        assertFunction("array_duplicates(array[0, null])", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_duplicates(array[0, null, null])", new ArrayType(INTEGER), singletonList(null));

        RowType rowType = RowType.from(ImmutableList.of(RowType.field(INTEGER), RowType.field(INTEGER)));
        assertFunction("array_duplicates(array[array[1], array[2], array[]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of());
        assertFunction("array_duplicates(array[array[1], array[2], array[2]])", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(2)));
        assertFunction("array_duplicates(array[(1, 2), (1, 2)])", new ArrayType(rowType), ImmutableList.of(ImmutableList.of(1, 2)));
        assertFunction("array_duplicates(array[(1, 2), (2, 2)])", new ArrayType(rowType), ImmutableList.of());
        assertInvalidFunction("array_duplicates(array[(1, null), (null, 2), (null, 1)])", StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
        assertInvalidFunction("array_duplicates(array[(1, null), (null, 2), (null, null)])", StandardErrorCode.NOT_SUPPORTED, "map key cannot be null or contain nulls");
    }

    @Test
    public void testArrayLeastFrequentBaseCase()
    {
        // Base Case
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [1, 2, 2, 3, 3, 3])", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY ['a', 'b', 'b', 'c', 'c', 'c'])", new ArrayType(createVarcharType(1)), ImmutableList.of("a"));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [1, 1, 2, 2, 3, 3])", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [DOUBLE '1.0', DOUBLE '2.0', DOUBLE '3.0'])", new ArrayType(DOUBLE), asList(1.0d));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY ['abc', 'bc', 'aaa'])", new ArrayType(createVarcharType(3)), ImmutableList.of("aaa"));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY ['', '', ' '])", new ArrayType(createVarcharType(1)), ImmutableList.of(" "));
    }

    @Test
    public void testArrayLeastFrequentComplexAndEdgeCase()
    {
        // Empty Case
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [])", new ArrayType(UNKNOWN), null);
        // Null Case
        assertFunction("ARRAY_LEAST_FREQUENT(null)", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [NULL])", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [1, 2, 2, NULL])", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [NULL, NULL, NULL])", new ArrayType(UNKNOWN), null);
        // Complex Case
        RowType rowType = RowType.from(ImmutableList.of(RowType.field(INTEGER), RowType.field(INTEGER)));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [ROW(1, 2), ROW(2, 3), ROW(2, 3)])", new ArrayType(rowType), ImmutableList.of(ImmutableList.of(1, 2)));
    }

    @Test
    public void testArrayNLeastFrequentBaseCase()
    {
        // Base Case
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [1, 2, 2, 3, 3, 3], 2)", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY ['a', 'b', 'b', 'c', 'c', 'c'], 3)", new ArrayType(createVarcharType(1)), ImmutableList.of("a", "b", "c"));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [1, 1, 2, 2, 3, 3], 1)", new ArrayType(INTEGER), ImmutableList.of(1));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [DOUBLE '1.0', DOUBLE '2.0', DOUBLE '3.0'], 2)", new ArrayType(DOUBLE), asList(1.0d, 2.0d));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY ['abc', 'bc', 'aaa'], 3)", new ArrayType(createVarcharType(3)), ImmutableList.of("aaa", "abc", "bc"));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY ['', '', ' '], 1)", new ArrayType(createVarcharType(1)), ImmutableList.of(" "));
    }

    @Test
    public void testArrayNLeastFrequentEmptyAndNullCase()
    {
        // Empty Case
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [], 2)", new ArrayType(UNKNOWN), null);
        // Null Case
        assertFunction("ARRAY_LEAST_FREQUENT(null, 3)", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [NULL], 0)", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [NULL, NULL, NULL], 1)", new ArrayType(UNKNOWN), null);
    }

    @Test
    public void testArrayNLeastFrequentZeroAndComplexCase()
    {
        // N = 0
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [1, 2, 2, NULL], 0)", new ArrayType(INTEGER), emptyList());
        // N < 0
        assertInvalidFunction("ARRAY_LEAST_FREQUENT(ARRAY ['a', 'b', 'b', 'c', 'c', 'c'], -1)", StandardErrorCode.GENERIC_USER_ERROR, "n must be greater than or equal to 0");
        // N greater distinct non-null elements in the array
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [1, 2, 2, 3, 3, 3, -1], 5)", new ArrayType(INTEGER), ImmutableList.of(-1, 1, 2, 3));
        // Complex Case
        RowType rowType = RowType.from(ImmutableList.of(RowType.field(INTEGER), RowType.field(INTEGER)));
        assertFunction("ARRAY_LEAST_FREQUENT(ARRAY [ROW(1, 2), ROW(2, 3), ROW(2, 3)], 2)", new ArrayType(rowType), ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(2, 3)));
    }

    @Test
    public void testArrayMaxBy()
    {
        assertFunction("ARRAY_MAX_BY(ARRAY [double'1.0', double'2.0'], i -> i)", DOUBLE, 2.0d);
        assertFunction("ARRAY_MAX_BY(ARRAY [double'-3.0', double'2.0'], i -> i*i)", DOUBLE, -3.0d);
        assertFunction("ARRAY_MAX_BY(ARRAY ['a', 'bb', 'c'], x -> LENGTH(x))", createVarcharType(2), "bb");
        assertFunction("ARRAY_MAX_BY(ARRAY [1, 2, 3], x -> 1-x)", INTEGER, 1);
        assertFunction("ARRAY_MAX_BY(ARRAY [ARRAY['a'], ARRAY['b', 'b'], ARRAY['c']], x -> CARDINALITY(x))", new ArrayType(createVarcharType(1)), asList("b", "b"));
        assertFunction("ARRAY_MAX_BY(ARRAY [MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]), MAP(ARRAY['foo', 'bar'], ARRAY[0, 3])], x -> x['foo'])", mapType(createVarcharType(3), INTEGER), ImmutableMap.of("foo", 1, "bar", 2));
        assertFunction("ARRAY_MAX_BY(ARRAY [CAST(ROW(0, 2.0) AS ROW(x BIGINT, y DOUBLE)), CAST(ROW(1, 3.0) AS ROW(x BIGINT, y DOUBLE))], r -> r.y).x", BIGINT, 1L);
        assertFunction("ARRAY_MAX_BY(ARRAY [null, double'1.0', double'2.0'], i -> i)", DOUBLE, null);
        assertFunction("ARRAY_MAX_BY(ARRAY [cast(null as double), cast(null as double)], i -> i)", DOUBLE, null);
        assertFunction("ARRAY_MAX_BY(cast(null as array(double)), i -> i)", DOUBLE, null);
    }

    @Test
    public void testArrayMinBy()
    {
        assertFunction("ARRAY_MIN_BY(ARRAY [double'1.0', double'2.0'], i -> i)", DOUBLE, 1.0d);
        assertFunction("ARRAY_MIN_BY(ARRAY [double'-3.0', double'2.0'], i -> i*i)", DOUBLE, 2.0d);
        assertFunction("ARRAY_MIN_BY(ARRAY ['a', 'bb', 'c'], x -> LENGTH(x))", createVarcharType(2), "a");
        assertFunction("ARRAY_MIN_BY(ARRAY [1, 2, 3], x -> 1-x)", INTEGER, 3);
        assertFunction("ARRAY_MIN_BY(ARRAY [ARRAY['a'], ARRAY['b', 'b'], ARRAY['c']], x -> CARDINALITY(x))", new ArrayType(createVarcharType(1)), singletonList("a"));
        assertFunction("ARRAY_MIN_BY(ARRAY [MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]), MAP(ARRAY['foo', 'bar'], ARRAY[0, 3])], x -> x['foo'])", mapType(createVarcharType(3), INTEGER), ImmutableMap.of("foo", 0, "bar", 3));
        assertFunction("ARRAY_MIN_BY(ARRAY [CAST(ROW(0, 2.0) AS ROW(x BIGINT, y DOUBLE)), CAST(ROW(1, 3.0) AS ROW(x BIGINT, y DOUBLE))], r -> r.y).x", BIGINT, 0L);
        assertFunction("ARRAY_MIN_BY(ARRAY [null, double'1.0', double'2.0'], i -> i)", DOUBLE, null);
        assertFunction("ARRAY_MIN_BY(ARRAY [cast(null as double), cast(null as double)], i -> i)", DOUBLE, null);
        assertFunction("ARRAY_MIN_BY(cast(null as array(double)), i -> i)", DOUBLE, null);
    }

    @Test
    public void testArraySortDescNumeric()
    {
        assertFunction("ARRAY_SORT_DESC(ARRAY [100, 1, 10, 50])", new ArrayType(INTEGER), ImmutableList.of(100, 50, 10, 1));
        assertFunction("ARRAY_SORT_DESC(ARRAY [null, null, 100, 1, 10, 50])", new ArrayType(INTEGER), asList(100, 50, 10, 1, null, null));
        assertFunction("ARRAY_SORT_DESC(ARRAY [double'1.0', double'2.0'])", new ArrayType(DOUBLE), ImmutableList.of(2.0d, 1.0d));
        assertFunction("ARRAY_SORT_DESC(ARRAY [double'1.0', double'2.0'])", new ArrayType(DOUBLE), ImmutableList.of(2.0d, 1.0d));
    }

    @Test
    public void testArraySortDescVarcharTypes()
    {
        assertFunction("ARRAY_SORT_DESC(ARRAY [null, double'-3.0', double'2.0', null])", new ArrayType(DOUBLE), asList(2.0d, -3.0d, null, null));
        assertFunction("ARRAY_SORT_DESC(ARRAY ['a', 'bb', 'c'])", new ArrayType(createVarcharType(2)), ImmutableList.of("c", "bb", "a"));
        assertFunction("ARRAY_SORT_DESC(ARRAY ['a', 'bb', 'c', null])", new ArrayType(createVarcharType(2)), asList("c", "bb", "a", null));
        assertFunction("ARRAY_SORT_DESC(ARRAY [null, null, null])", new ArrayType(UNKNOWN), asList(null, null, null));
        assertFunction("ARRAY_SORT_DESC(ARRAY [])", new ArrayType(UNKNOWN), emptyList());
        assertFunction("ARRAY_SORT_DESC(null)", new ArrayType(UNKNOWN), null);
        assertFunction("ARRAY_SORT_DESC(" +
                        "ARRAY [ARRAY['a'], ARRAY['b', 'b'], ARRAY['c']])",
                new ArrayType(new ArrayType(createVarcharType(1))),
                ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("b", "b"), ImmutableList.of("a")));
        assertFunction("ARRAY_SORT_DESC(" +
                        "ARRAY [ARRAY['a'], ARRAY['b', 'b'], ARRAY['c'], null, null, ARRAY['a', NULL]])",
                new ArrayType(new ArrayType(createVarcharType(1))),
                asList(singletonList("c"), ImmutableList.of("b", "b"), asList("a", null), singletonList("a"), null, null));
        assertInvalidFunction("ARRAY_SORT_DESC(ARRAY [ROW('a', 1), ROW('a', null), null, ROW('a', 0)])", StandardErrorCode.INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("ARRAY_SORT_DESC(ARRAY [MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]), MAP(ARRAY['foo', 'bar'], ARRAY[0, 3])])", SemanticErrorCode.FUNCTION_NOT_FOUND);
    }

    @Test
    public void testArrayTopNNumeric()
    {
        // Test INT, DOUBLE, and mixed
        assertFunction("ARRAY_TOP_N(ARRAY [1, 1, 1, 1], 3)", new ArrayType(INTEGER), ImmutableList.of(1, 1, 1));
        assertFunction("ARRAY_TOP_N(ARRAY [1, 100, 2, 5, 3], 3)", new ArrayType(INTEGER), ImmutableList.of(100, 5, 3));
        assertFunction("ARRAY_TOP_N(ARRAY [DOUBLE '1.0', DOUBLE '100.0', DOUBLE '2.0', DOUBLE '5.0', DOUBLE '3.0'], 3)", new ArrayType(DOUBLE), ImmutableList.of(100.0d, 5.0d, 3.0d));
        assertFunction("ARRAY_TOP_N(ARRAY [DOUBLE '1.0', 100, 2, DOUBLE '5.0', DOUBLE '3.0'], 3)", new ArrayType(DOUBLE), ImmutableList.of(100d, 5.0d, 3.0d));
        assertFunction("ARRAY_TOP_N(ARRAY [1, 4, null], 3)", new ArrayType(INTEGER), asList(4, 1, null));
    }

    @Test
    public void testArrayTopNVarchar()
    {
        assertFunction("ARRAY_TOP_N(ARRAY ['a', 'z', 'd', 'f', 'g', 'b'], 4)", new ArrayType(createVarcharType(1)), ImmutableList.of("z", "g", "f", "d"));
        assertFunction("ARRAY_TOP_N(ARRAY ['foo', 'bar', 'lorem', 'ipsum', 'lorem2'], 3)", new ArrayType(createVarcharType(6)), ImmutableList.of("lorem2", "lorem", "ipsum"));
        assertFunction("ARRAY_TOP_N(ARRAY ['a', 'zzz', 'zz', 'b', 'g', 'f'], 3)", new ArrayType(createVarcharType(3)), ImmutableList.of("zzz", "zz", "g"));
        assertFunction("ARRAY_TOP_N(ARRAY ['a', 'a', 'd', 'a', 'a', 'a'], 3)", new ArrayType(createVarcharType(1)), ImmutableList.of("d", "a", "a"));
    }

    @Test
    public void testArrayTopNBooleanAndComparatorTypes()
    {
        // Test BOOLEAN
        assertFunction("ARRAY_TOP_N(ARRAY [true, true, false, true, false], 4)", new ArrayType(BOOLEAN), ImmutableList.of(true, true, true, false));

        // Test comparator function
        assertFunction("ARRAY_TOP_N(ARRAY [100, 1, 3, -10, 6, -5], 3, (x, y) -> IF(abs(x) < abs(y), -1, IF(abs(x) = abs(y), 0, 1)))", new ArrayType(INTEGER), ImmutableList.of(100, -10, 6));

        RowType rowType = RowType.from(ImmutableList.of(RowType.field("x", INTEGER), RowType.field("y", INTEGER)));
        assertFunction("ARRAY_TOP_N(ARRAY [CAST(ROW(1, 2) AS ROW(x INT, y INT)), CAST(ROW(0, 11) AS ROW(x INT, y INT)), CAST(ROW(5, 10) AS ROW(x INT, y INT))], 2, (a, b) -> IF(a.x*a.y < b.x*b.y, -1, IF(a.x*a.y = b.x*b.y, 0, 1)))", new ArrayType(rowType), ImmutableList.of(ImmutableList.of(5, 10), ImmutableList.of(1, 2)));
    }

    @Test
    public void testArrayTopNEdgeAndErrorCase()
    {
        // Test exceptions
        assertInvalidFunction("ARRAY_TOP_N(ARRAY [ROW('a', 1), ROW('a', null), null, ROW('a', 0)], 2)", StandardErrorCode.INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("ARRAY_TOP_N(ARRAY [MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]), MAP(ARRAY['foo', 'bar'], ARRAY[0, 3])], 2)", SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction("ARRAY_TOP_N(ARRAY ['a', 'a', 'd', 'a', 'a', 'a'], -1)", StandardErrorCode.GENERIC_USER_ERROR, "Parameter n: -1 to ARRAY_TOP_N is negative");

        // Test edge cases
        assertFunction("ARRAY_TOP_N(ARRAY [null, null], 3)", new ArrayType(UNKNOWN), asList(null, null));
        assertFunction("ARRAY_TOP_N(ARRAY [3, 5, 1, 2], 0)", new ArrayType(INTEGER), emptyList());
        assertFunction("ARRAY_TOP_N(ARRAY [], 3)", new ArrayType(UNKNOWN), emptyList());
        assertFunction("ARRAY_TOP_N(ARRAY [1, 4], 3)", new ArrayType(INTEGER), ImmutableList.of(4, 1));
    }
}
