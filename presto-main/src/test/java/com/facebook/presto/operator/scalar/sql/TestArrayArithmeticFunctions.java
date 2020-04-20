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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public class TestArrayArithmeticFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testArraySum()
    {
        assertFunction("array_sum(array[BIGINT '1', BIGINT '2'])", BIGINT, 3L);
        assertFunction("array_sum(array[INTEGER '1', INTEGER '2'])", BIGINT, 3L);
        assertFunction("array_sum(array[SMALLINT '1', SMALLINT '2'])", BIGINT, 3L);
        assertFunction("array_sum(array[TINYINT '1', TINYINT '2'])", BIGINT, 3L);

        assertFunction("array_sum(array[BIGINT '1', INTEGER '2'])", BIGINT, 3L);
        assertFunction("array_sum(array[INTEGER '1', SMALLINT '2'])", BIGINT, 3L);
        assertFunction("array_sum(array[SMALLINT '1', TINYINT '2'])", BIGINT, 3L);

        assertFunctionWithError("array_sum(array[DOUBLE '-2.0', DOUBLE '5.3'])", DOUBLE, 3.3);
        assertFunctionWithError("array_sum(array[DOUBLE '-2.0', REAL '5.3'])", DOUBLE, 3.3);
        assertFunctionWithError("array_sum(array[DOUBLE '-2.0', DECIMAL '5.3'])", DOUBLE, 3.3);
        assertFunctionWithError("array_sum(array[REAL '-2.0', DECIMAL '5.3'])", DOUBLE, 3.3);

        assertFunctionWithError("array_sum(array[BIGINT '-2', DOUBLE '5.3'])", DOUBLE, 3.3);
        assertFunctionWithError("array_sum(array[INTEGER '-2', REAL '5.3'])", DOUBLE, 3.3);
        assertFunctionWithError("array_sum(array[SMALLINT '-2', DECIMAL '5.3'])", DOUBLE, 3.3);
        assertFunctionWithError("array_sum(array[TINYINT '-2', DOUBLE '5.3'])", DOUBLE, 3.3);

        assertFunction("array_sum(null)", BIGINT, null);
        assertFunction("array_sum(array[])", BIGINT, 0L);
        assertFunction("array_sum(array[NULL])", BIGINT, 0L);
        assertFunction("array_sum(array[NULL, NULL, NULL])", BIGINT, 0L);
        assertFunction("array_sum(array[3, NULL, 5])", BIGINT, 8L);
        assertFunctionWithError("array_sum(array[NULL, double '1.2', double '2.3', NULL, -3])", DOUBLE, 0.5);
    }

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
}
