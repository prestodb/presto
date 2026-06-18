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
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;

public class CoalesceCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        Variable wasNull = generatorContext.wasNull();
        LabelNode endLabel = new LabelNode("end");
        BytecodeBlock block = new BytecodeBlock();

        // Generate all but the last one.
        for (RowExpression expression : arguments.subList(0, arguments.size() - 1)) {
            block.append(generatorContext.generate(expression, Optional.empty()));
            // Check if null
            IfStatement ifStatement = new IfStatement().condition(wasNull);
            ifStatement.ifTrue()
                    .pop(returnType.getJavaType())
                    .append(wasNull.set(constantFalse()));

            ifStatement.ifFalse().gotoLabel(endLabel);
            block.append(ifStatement);
        }

        // Just return the last one here if control reaches here.
        block.append(generatorContext.generate(arguments.get(arguments.size() - 1), Optional.empty()));
        block.visitLabel(endLabel);
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generatorContext, returnType, output)));
        return block;
    }
}
