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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.type.TypeCalculation.calculateLiteralValue;
import static org.testng.Assert.assertEquals;

public class TestTypeCalculation
{
    @Test
    public void testBasicUsage()
    {
        assertEquals(calculateLiteralValue("42", ImmutableMap.of()), Long.valueOf(42));
        assertEquals(calculateLiteralValue("NULL", ImmutableMap.of()), Long.valueOf(0));
        assertEquals(calculateLiteralValue("null", ImmutableMap.of()), Long.valueOf(0));
        assertEquals(calculateLiteralValue("x", ImmutableMap.of("x", 42L)), Long.valueOf(42));
        assertEquals(calculateLiteralValue("(42)", ImmutableMap.of()), Long.valueOf(42));
        assertEquals(calculateLiteralValue("(NULL)", ImmutableMap.of()), Long.valueOf(0));
        assertEquals(calculateLiteralValue("(x)", ImmutableMap.of("x", 42L)), Long.valueOf(42));

        assertEquals(calculateLiteralValue("42 + 55", ImmutableMap.of()), Long.valueOf(42 + 55));
        assertEquals(calculateLiteralValue("42 - 55", ImmutableMap.of()), Long.valueOf(42 - 55));
        assertEquals(calculateLiteralValue("42 * 55", ImmutableMap.of()), Long.valueOf(42 * 55));
        assertEquals(calculateLiteralValue("42 / 6", ImmutableMap.of()), Long.valueOf(42 / 6));

        assertEquals(calculateLiteralValue("42 + 55 * 6", ImmutableMap.of()), Long.valueOf(42 + 55 * 6));
        assertEquals(calculateLiteralValue("(42 + 55) * 6", ImmutableMap.of()), Long.valueOf((42 + 55) * 6));

        assertEquals(calculateLiteralValue("min(10,2)", ImmutableMap.of()), Long.valueOf(2));
        assertEquals(calculateLiteralValue("min(10,2*10)", ImmutableMap.of()), Long.valueOf(10));
        assertEquals(calculateLiteralValue("max(10,2*10)", ImmutableMap.of()), Long.valueOf(20));
        assertEquals(calculateLiteralValue("max(10,2)", ImmutableMap.of()), Long.valueOf(10));

        assertEquals(calculateLiteralValue("x + y", ImmutableMap.of("x", 42L, "y", 55L)), Long.valueOf(42 + 55));
    }
}
