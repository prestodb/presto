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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;

public class TestNewInstanceBytecodeExpression
{
    @Test
    public void testNewInstance()
            throws Exception
    {
        assertBytecodeExpression(newInstance(UUID.class, constantLong(3), constantLong(7)), new UUID(3L, 7L), "new UUID(3L, 7L)");
        assertBytecodeExpression(
                newInstance(UUID.class, ImmutableList.of(long.class, long.class), constantLong(3), constantLong(7)),
                new UUID(3L, 7L),
                "new UUID(3L, 7L)");
    }
}
