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
package com.facebook.presto.bytecode.expression;

import org.testng.annotations.Test;

import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static org.testng.Assert.assertEquals;

public class TestPopBytecodeExpression
{
    @Test
    public void testGetField()
            throws Exception
    {
        intCount = 0;
        assertBytecodeExpression(invokeStatic(getClass(), "incrementAndGetIntCount", int.class).pop(), null, getClass().getSimpleName() + ".incrementAndGetIntCount();");
        assertEquals(intCount, 1);
        longCount = 0;
        assertBytecodeExpression(invokeStatic(getClass(), "incrementAndGetLongCount", long.class).pop(), null, getClass().getSimpleName() + ".incrementAndGetLongCount();");
        assertEquals(longCount, 1);
    }

    private static int intCount;

    public static int incrementAndGetIntCount()
    {
        return intCount++;
    }

    private static long longCount;

    public static long incrementAndGetLongCount()
    {
        return longCount++;
    }
}
