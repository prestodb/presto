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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;

public class CoalesceCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        List<BytecodeNode> operands = new ArrayList<>();
        for (RowExpression expression : arguments) {
            operands.add(generatorContext.generate(expression, Optional.empty()));
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

        BytecodeBlock block = new BytecodeBlock()
                .append(nullValue);
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generatorContext, returnType, output)));
        return block;
    }
}
