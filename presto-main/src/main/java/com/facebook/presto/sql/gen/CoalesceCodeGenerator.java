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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class CoalesceCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        List<ByteCodeNode> operands = new ArrayList<>();
        for (RowExpression expression : arguments) {
            operands.add(generatorContext.generate(expression));
        }

        CompilerContext context = generatorContext.getContext();
        ByteCodeNode nullValue = new Block(context)
                .putVariable(context.getVariable("wasNull"), true)
                .pushJavaDefault(returnType.getJavaType());

        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (ByteCodeNode operand : Lists.reverse(operands)) {
            IfStatement ifStatement = new IfStatement();

            ifStatement.condition()
                    .append(operand)
                    .append(context.getVariable("wasNull"));

            // if value was null, pop the null value, clear the null flag, and process the next operand
            ifStatement.ifTrue()
                    .pop(returnType.getJavaType())
                    .putVariable(context.getVariable("wasNull"), false)
                    .append(nullValue);

            nullValue = ifStatement;
        }

        return nullValue;
    }
}
