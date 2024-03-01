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
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.google.common.base.Preconditions.checkArgument;

public class DereferenceCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generator, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        checkArgument(arguments.size() == 2);
        CallSiteBinder callSiteBinder = generator.getCallSiteBinder();

        // Collect all nested intermediateDerefs
        ImmutableList.Builder<RowExpression> nestedDerefernces = ImmutableList.builder();
        RowExpression nestedObject = arguments.get(0);
        int leafFieldIndex = ((Number) ((ConstantExpression) arguments.get(1)).getValue()).intValue();

        // Find all the intermediate nestedDerefernces.
        while (nestedObject instanceof SpecialFormExpression &&
                ((SpecialFormExpression) nestedObject).getForm() == DEREFERENCE) {
            nestedDerefernces.add(nestedObject);
            nestedObject = ((SpecialFormExpression) nestedObject).getArguments().get(0);
        }

        // Here nestedObject is the inner-most expression (so the toplevel object)
        // Just generate a loop
        BytecodeBlock block = new BytecodeBlock().comment("DEREFERENCE").setDescription("DEREFERENCE");
        Variable rowBlock = generator.getScope().createTempVariable(Block.class);
        Variable wasNull = generator.wasNull();

        // Labels for control-flow
        LabelNode end = new LabelNode("end");
        LabelNode returnNull = new LabelNode("returnNull");

        // clear the wasNull flag before evaluating the row value and evaluate the root (innermost) object
        block.putVariable(wasNull, false)
                .append(generator.generate(nestedObject, Optional.empty()))
                .putVariable(rowBlock)
                .comment("If the object is null return null")
                .append(wasNull)
                .ifTrueGoto(returnNull);

        for (RowExpression rowExpression : nestedDerefernces.build().reverse()) {
            SpecialFormExpression nestedDerefernce = (SpecialFormExpression) rowExpression;
            int fieldIndex = ((Number) ((ConstantExpression) nestedDerefernce.getArguments().get(1)).getValue()).intValue();
            block.append(rowBlock)
                    .push(fieldIndex)
                    .invokeInterface(Block.class, "isNull", boolean.class, int.class)
                    .comment("If the deref result is null return null")
                    .ifTrueGoto(returnNull)
                    .append(constantType(callSiteBinder, nestedDerefernce.getType()).getValue(rowBlock, constantInt(fieldIndex)))
                    .putVariable(rowBlock);
        }

        block.append(rowBlock)
                .push(((Number) ((ConstantExpression) arguments.get(1)).getValue()).intValue())
                .invokeInterface(Block.class, "isNull", boolean.class, int.class)
                .ifTrueGoto(returnNull);

        BytecodeExpression value = constantType(callSiteBinder, returnType).getValue(rowBlock, constantInt(leafFieldIndex));
        block.append(wasNull)
                .ifTrueGoto(returnNull)
                .append(value)
                .gotoLabel(end);

        // Here one of the enclosing objects is null
        Class<?> javaType = returnType.getJavaType();
        block.visitLabel(returnNull)
                .pushJavaDefault(javaType)
                .comment("if the field is null, push null to stack")
                .putVariable(wasNull, true);

        block.visitLabel(end);
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(generator, returnType, output)));
        return block;
    }
}
