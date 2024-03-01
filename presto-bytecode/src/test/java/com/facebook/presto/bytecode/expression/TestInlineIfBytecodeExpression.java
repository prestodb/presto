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
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.inlineIf;

public class TestInlineIfBytecodeExpression
{
    @Test
    public void testInlineIf()
            throws Exception
    {
        assertBytecodeExpression(inlineIf(constantTrue(), constantString("T"), constantString("F")), true ? "T" : "F", "(true ? \"T\" : \"F\")");
        assertBytecodeExpression(inlineIf(constantFalse(), constantString("T"), constantString("F")), false ? "T" : "F", "(false ? \"T\" : \"F\")");
    }
}
