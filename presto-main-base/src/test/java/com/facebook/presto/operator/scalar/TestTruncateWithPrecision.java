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

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;

public class TestTruncateWithPrecision
        extends AbstractTestFunctions
{
    @Test
    public void testTruncate()
    {
        // TRUNCATE DOUBLE -> DOUBLE
        assertFunction("truncate(nan(),-1)", DOUBLE, Double.NaN);
        assertFunction("truncate(infinity(),1)", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("truncate(-infinity(),1)", DOUBLE, Double.NEGATIVE_INFINITY);
        assertFunction("truncate(DOUBLE '17.1', -1)", DOUBLE, 10.0);
        assertFunction("truncate(DOUBLE '1234.56', 1)", DOUBLE, 1234.5);
        assertFunction("truncate(DOUBLE '1234.56', 0)", DOUBLE, 1234.0);
        assertFunction("truncate(DOUBLE '-1234.56', 0)", DOUBLE, -1234.0);
        assertFunction("truncate(DOUBLE '-1234.56', -500)", DOUBLE, 0.0);
        assertFunction("truncate(DOUBLE '1234.567', 2)", DOUBLE, 1234.56);
        assertFunction("truncate(DOUBLE '1234.567', 2)", DOUBLE, 1234.56);
        assertFunction("truncate(DOUBLE '" + Double.MAX_VALUE + "', -408)", DOUBLE, 0.0);
        assertFunction("truncate(DOUBLE '" + -Double.MAX_VALUE / 10 + "', 408)", DOUBLE, -Double.MAX_VALUE / 10);
        assertFunction("truncate(DOUBLE '" + (double) Long.MAX_VALUE + "', -15)", DOUBLE, 9.223E18);

        // TRUNCATE REAL -> REAL

        assertFunction("truncate(REAL '12.333', 0)", REAL, 12.0f);
        assertFunction("truncate(REAL '-12.333', 0)", REAL, -12.0f);
        assertFunction("truncate(REAL '12.123456789011234567892', 10)", REAL, 12.12345695499999f);
        assertFunction("truncate(REAL '12.333', -1)", REAL, 10.0f);
        assertFunction("truncate(REAL '12.333', -500)", REAL, 0.0f);
        assertFunction("truncate(REAL '3.40287e37', -35)", REAL, 3.4E37f);
        assertFunction("truncate(REAL '3.40287e37', -488)", REAL, 0.0f);
        assertFunction("truncate(REAL '" + (float) Long.MAX_VALUE + "', -15)", REAL, 9.223E18f);
    }
}
