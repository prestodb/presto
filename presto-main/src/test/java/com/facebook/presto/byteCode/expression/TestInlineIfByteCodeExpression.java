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
package com.facebook.presto.byteCode.expression;

import org.testng.annotations.Test;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpression;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFalse;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.inlineIf;

public class TestInlineIfByteCodeExpression
{
    @Test
    public void testInlineIf()
            throws Exception
    {
        assertByteCodeExpression(inlineIf(constantTrue(), constantString("T"), constantString("F")), true ? "T" : "F", "(true ? \"T\" : \"F\")");
        assertByteCodeExpression(inlineIf(constantFalse(), constantString("T"), constantString("F")), false ? "T" : "F", "(false ? \"T\" : \"F\")");
    }
}
