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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.type.ArrayType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.asList;

public class TestArrayReduceFunction
        extends AbstractTestFunctions
{
    public TestArrayReduceFunction()
    {
        super(testSessionBuilder().setTimeZoneKey(getTimeZoneKey("Pacific/Kiritimati")).build());
    }

    @Test
    public void testEmpty()
    {
        assertFunction("reduce(ARRAY [], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)", BIGINT, 0L);
    }

    @Test
    public void testBasic()
    {
        assertFunction("reduce(ARRAY [5, 20, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)", BIGINT, 75L);
        assertFunction("reduce(ARRAY [5 + RANDOM(1), 20, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)", BIGINT, 75L);
        assertFunction("reduce(ARRAY [5, 6, 10, 20], 0.0E0, (s, x) -> s + x, s -> s)", DOUBLE, 41.0);
    }

    @Test
    public void testNulls()
    {
        assertFunction("reduce(ARRAY [NULL], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)", BIGINT, null);
        assertFunction("reduce(ARRAY [NULL, NULL, NULL], CAST (0 AS BIGINT), (s, x) -> coalesce(x, 1) + s, s -> s)", BIGINT, 3L);
        assertFunction("reduce(ARRAY [5, NULL, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s)", BIGINT, null);
        assertFunction("reduce(ARRAY [5, NULL, 50], CAST (0 AS BIGINT), (s, x) -> coalesce(x, 0) + s, s -> s)", BIGINT, 55L);

        // mimics max function
        assertFunction("reduce(ARRAY [], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)", BIGINT, null);
        assertFunction("reduce(ARRAY [NULL], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)", BIGINT, null);
        assertFunction("reduce(ARRAY [NULL, NULL, NULL], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)", BIGINT, null);
        assertFunction("reduce(ARRAY [NULL, 6, 10, NULL, 8], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)", BIGINT, 10L);
        assertFunction("reduce(ARRAY [5, NULL, 6, 10, NULL, 8], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s)", BIGINT, 10L);
    }

    @Test
    public void testTwoValueState()
    {
        assertFunction(
                "reduce(" +
                        "ARRAY [5, 20, 50], " +
                        "CAST(ROW(0, 0) AS ROW(sum BIGINT, count INTEGER)), " +
                        "(s, x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum BIGINT, count INTEGER)), " +
                        "s -> s.sum / s.count)",
                BIGINT,
                25L);
        assertFunction(
                "reduce(" +
                        "ARRAY [5, 6, 10, 20], " +
                        "CAST(ROW(0.0E0, 0) AS ROW(sum DOUBLE, count INTEGER)), " +
                        "(s, x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), " +
                        "s -> s.sum / s.count)",
                DOUBLE,
                10.25);
    }

    @Test
    public void testInstanceFunction()
    {
        assertFunction(
                "reduce(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, NULL, 7]], CAST(ARRAY[] AS ARRAY(INTEGER)), (s, x) -> concat(s, x), s -> s)",
                new ArrayType(INTEGER),
                asList(1, 2, 3, 4, 5, null, 7));
    }

    @Test
    public void testCoercion()
    {
        assertFunction("reduce(ARRAY [123456789012345, NULL, 54321], 0, (s, x) -> s + coalesce(x, 0), s -> s)", BIGINT, 123456789066666L);
        // TODO: Support coercion of return type of lambda
        assertInvalidFunction("reduce(ARRAY [1, NULL, 2], 0, (s, x) -> CAST (s + x AS TINYINT), s -> s)", FUNCTION_NOT_FOUND);
    }
}
