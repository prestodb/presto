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
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;

public class OrCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generator, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        Preconditions.checkArgument(arguments.size() == 2);

        // We flatten the AND here.
        Deque<RowExpression> stack = new ArrayDeque<RowExpression>();
        stack.push(arguments.get(1));
        stack.push(arguments.get(0));

        ImmutableList.Builder<RowExpression> flattenedArgs = ImmutableList.builder();
        do {
            RowExpression operand = stack.pop();
            if (operand instanceof SpecialFormExpression &&
                    ((SpecialFormExpression) operand).getForm() == SpecialFormExpression.Form.OR) {
                stack.push(((SpecialFormExpression) operand).getArguments().get(1));
                stack.push(((SpecialFormExpression) operand).getArguments().get(0));
            }
            else {
                flattenedArgs.add(operand);
            }
        } while (!stack.isEmpty());

        BytecodeBlock block = new BytecodeBlock()
                .comment("OR")
                .setDescription("OR");

        LabelNode trueLabel = new LabelNode("true");
        LabelNode endLabel = new LabelNode("end");
        Variable wasNull = generator.wasNull();

        Variable hasNulls = generator.getScope().createTempVariable(boolean.class);
        block.initializeVariable(hasNulls);
        for (RowExpression expression : flattenedArgs.build()) {
            block.comment("do { eval arg; if (wasNull) { hasNull = true; wasNull = false; } else if (true) goto ret_true; }")
                    .append(generator.generate(expression, Optional.empty()));
            IfStatement ifOperandIsNull = new IfStatement("if left wasNulll...").condition(wasNull);
            ifOperandIsNull.ifTrue()
                    .comment("clear the null flag and remember there was a null")
                    .putVariable(hasNulls, true)
                    .putVariable(wasNull, false)
                    .pop(boolean.class);

            ifOperandIsNull.ifFalse()
                    .ifTrueGoto(trueLabel);

            block.append(ifOperandIsNull);
        }

        // We evaluated all operands. So check if any of them was null
        IfStatement ifHasNulls = new IfStatement("hasNulls is true");
        ifHasNulls.condition().append(hasNulls);
        ifHasNulls.ifTrue()
                .comment("at least one of the arguments is null and none of them is true. So set wasNull to true")
                .putVariable(wasNull, true)
                .push(false);
        ifHasNulls.ifFalse().push(false);

        block.append(ifHasNulls)
                .gotoLabel(endLabel);

        block.visitLabel(trueLabel)
                .comment("at least one of the args is true, clear wasNull and return true")
                .push(true)
                .gotoLabel(endLabel);

        block.visitLabel(endLabel);
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generator, returnType, output)));
        return block;
    }
}
