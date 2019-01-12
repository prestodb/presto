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
package io.prestosql.sql.gen;

import com.google.common.collect.Lists;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;

public class CoalesceCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        List<BytecodeNode> operands = new ArrayList<>();
        for (RowExpression expression : arguments) {
            operands.add(generatorContext.generate(expression));
        }

        Variable wasNull = generatorContext.wasNull();
        BytecodeNode nullValue = new BytecodeBlock()
                .append(wasNull.set(constantTrue()))
                .pushJavaDefault(returnType.getJavaType());

        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (BytecodeNode operand : Lists.reverse(operands)) {
            IfStatement ifStatement = new IfStatement();

            ifStatement.condition()
                    .append(operand)
                    .append(wasNull);

            // if value was null, pop the null value, clear the null flag, and process the next operand
            ifStatement.ifTrue()
                    .pop(returnType.getJavaType())
                    .append(wasNull.set(constantFalse()))
                    .append(nullValue);

            nullValue = ifStatement;
        }

        return nullValue;
    }
}
