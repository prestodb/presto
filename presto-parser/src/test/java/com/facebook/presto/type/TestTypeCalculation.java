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

import java.util.OptionalLong;

import static com.facebook.presto.type.TypeCalculation.calculateLiteralValue;
import static org.testng.Assert.assertEquals;

public class TestTypeCalculation
{
    @Test
    public void basicUsage()
    {
        assertEquals(calculateLiteralValue("42", ImmutableMap.of()), OptionalLong.of(42));
        assertEquals(calculateLiteralValue("NULL", ImmutableMap.of()), OptionalLong.empty());
        assertEquals(calculateLiteralValue("null", ImmutableMap.of()), OptionalLong.empty());
        assertEquals(calculateLiteralValue("x", ImmutableMap.of("X", OptionalLong.of(42))), OptionalLong.of(42));
        assertEquals(calculateLiteralValue("(42)", ImmutableMap.of()), OptionalLong.of(42));
        assertEquals(calculateLiteralValue("(NULL)", ImmutableMap.of()), OptionalLong.empty());
        assertEquals(calculateLiteralValue("(x)", ImmutableMap.of("X", OptionalLong.of(42))), OptionalLong.of(42));

        assertEquals(calculateLiteralValue("42 + 55", ImmutableMap.of()), OptionalLong.of(42 + 55));
        assertEquals(calculateLiteralValue("42 - 55", ImmutableMap.of()), OptionalLong.of(42 - 55));
        assertEquals(calculateLiteralValue("42 * 55", ImmutableMap.of()), OptionalLong.of(42 * 55));
        assertEquals(calculateLiteralValue("42 / 6", ImmutableMap.of()), OptionalLong.of(42 / 6));

        assertEquals(calculateLiteralValue("42 + 55 * 6", ImmutableMap.of()), OptionalLong.of(42 + 55 * 6));
        assertEquals(calculateLiteralValue("(42 + 55) * 6", ImmutableMap.of()), OptionalLong.of((42 + 55) * 6));

        assertEquals(calculateLiteralValue("min(10,2)", ImmutableMap.of()), OptionalLong.of(2));
        assertEquals(calculateLiteralValue("min(10,2*10)", ImmutableMap.of()), OptionalLong.of(10));
        assertEquals(calculateLiteralValue("max(10,2*10)", ImmutableMap.of()), OptionalLong.of(20));
        assertEquals(calculateLiteralValue("max(10,2)", ImmutableMap.of()), OptionalLong.of(10));

        assertEquals(calculateLiteralValue("x + y", ImmutableMap.of("X", OptionalLong.of(42), "Y", OptionalLong.of(55))), OptionalLong.of(42 + 55));
        assertEquals(calculateLiteralValue("x + y", ImmutableMap.of("X", OptionalLong.of(42), "Y", OptionalLong.empty())), OptionalLong.empty());
    }

    @Test
    public void disallowExpressionFlag()
    {
        assertEquals(calculateLiteralValue("x", ImmutableMap.of("X", OptionalLong.of(42)), false), OptionalLong.of(42));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void failDisallowBinaryExpression()
    {
        calculateLiteralValue("x + y", ImmutableMap.of("X", OptionalLong.of(42), "Y", OptionalLong.of(55)), false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void failDisallowUnaryExpression()
    {
        calculateLiteralValue("-y", ImmutableMap.of("Y", OptionalLong.of(55)), false);
    }
}
