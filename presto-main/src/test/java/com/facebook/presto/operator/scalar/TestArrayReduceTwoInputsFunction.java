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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestArrayReduceTwoInputsFunction
        extends AbstractTestFunctions
{
    @Test
    public void testEmpty()
            throws Exception
    {
        assertFunction("reduce(ARRAY [], ARRAY [], CAST (0 AS BIGINT), (s, x, y) -> s + x + y, s -> s)", BIGINT, 0L);
    }

    @Test
    public void testBasic()
            throws Exception
    {
        assertFunction("reduce(ARRAY [5, 20, 50], ARRAY [1, 2, 3], CAST (0 AS BIGINT), (s, x, y) -> s + x * y, s -> s)", BIGINT, 195L);
        assertFunction("reduce(ARRAY [5 + RANDOM(1), 20, 50], ARRAY [1, 2, 3], CAST (0 AS BIGINT), (s, x, y) -> s + x * y, s -> s)", BIGINT, 195L);
        assertFunction("reduce(ARRAY [5, 6, 10, 20], ARRAY [1, 2, 3, 4], 0.0, (s, x, y) -> s + x * y, s -> s)", DOUBLE, 127.0);
        assertFunction("reduce(CAST(ARRAY['a', 'b'] AS ARRAY(VARCHAR)), CAST(ARRAY['1', '2'] AS ARRAY(VARCHAR)), CAST('/*' AS VARCHAR), (s, x, y) -> CONCAT(s, CONCAT(x, y)), s -> CONCAT(s, '*/'))", VARCHAR, "/*a1b2*/");
        assertFunction("reduce(CAST(ARRAY['a', 'b'] AS ARRAY(VARCHAR)), ARRAY[1, 2], CAST('/*' AS VARCHAR), (s, x, y) -> CONCAT(s, CONCAT(x, CAST(y AS VARCHAR))), s -> CONCAT(s, '*/'))", VARCHAR, "/*a1b2*/");
    }

    @Test
    public void testNulls()
            throws Exception
    {
        assertFunction("reduce(ARRAY [NULL], ARRAY [NULL], CAST (0 AS BIGINT), (s, x, y) -> s + x + y, s -> s)", BIGINT, null);
        assertFunction("reduce(ARRAY [NULL], ARRAY [1], CAST (0 AS BIGINT), (s, x, y) -> s + x + y, s -> s)", BIGINT, null);
        assertFunction("reduce(ARRAY [NULL, NULL, NULL], ARRAY [NULL, NULL, NULL], CAST (0 AS BIGINT), (s, x, y) -> coalesce(x, 1) + coalesce(y, 1) + s, s -> s)", BIGINT, 6L);
        assertFunction("reduce(ARRAY [5, 6, 10, 20], ARRAY [1, 2, 3], 0.0, (s, x, y) -> s + x * y, s -> s)", DOUBLE, null);
        assertFunction("reduce(ARRAY [5, 6, 10], ARRAY [1, 2, 3, 4], 0.0, (s, x, y) -> s + x * y, s -> s)", DOUBLE, null);
        assertFunction("reduce(ARRAY [5, 6, NULL, 20], ARRAY [1, 2, 3], 0.0, (s, x, y) -> s + coalesce(x, 1) * coalesce(y, 1), s -> s)", DOUBLE, 40.0);
    }

    @Test
    public void testMultipleValueState()
        throws Exception
    {
        assertFunction(
                "reduce(" +
                        "ARRAY [5, 20, 50], ARRAY [3, 6, 9]," +
                        "CAST(ROW(0, 0, 0) AS ROW(sumA BIGINT, sumB BIGINT, count INTEGER)), " +
                        "(s, x, y) -> CAST(ROW(x + s.sumA, y + s.sumB, s.count + 1) AS ROW(sumA BIGINT, sumB BIGINT, count INTEGER)), " +
                        "s -> IF(s.sumA > s.sumB, s.sumA / s.count, s.sumB / s.count))",
                BIGINT,
                25L);
    }
}
