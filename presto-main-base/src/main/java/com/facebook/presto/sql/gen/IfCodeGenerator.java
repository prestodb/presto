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
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;

public class IfCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    public BytecodeNode generateExpression(BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        Preconditions.checkArgument(arguments.size() == 3);

        Variable wasNull = context.wasNull();
        LabelNode falseLabel = new LabelNode("false");
        LabelNode endLabel = new LabelNode("end");
        BytecodeBlock block = new BytecodeBlock()
                .append(context.generate(arguments.get(0), Optional.empty()));

        IfStatement ifStatement = new IfStatement("... and condition value was not null")
                .condition(wasNull);

        ifStatement.ifTrue()
                .putVariable(wasNull, false)
                .pop(boolean.class)
                .gotoLabel(falseLabel);

        ifStatement.ifFalse()
                .ifFalseGoto(falseLabel)
                .append(context.generate(arguments.get(1), Optional.empty()))
                .gotoLabel(endLabel);

        block.append(ifStatement)
                .visitLabel(falseLabel)
                .append(context.generate(arguments.get(2), Optional.empty()));

        block.visitLabel(endLabel);
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(context, returnType, output)));
        return block;
    }
}
