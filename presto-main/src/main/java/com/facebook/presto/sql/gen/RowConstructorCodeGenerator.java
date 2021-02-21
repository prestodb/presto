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
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.SwitchStatement;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.SpecialFormBytecodeGenerator.generateWrite;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;

public class RowConstructorCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context, Type rowType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType.toString());
        CallSiteBinder binder = context.getCallSiteBinder();
        Scope scope = context.getScope();
        List<Type> types = rowType.getTypeParameters();

        block.comment("Create new RowBlockBuilder; beginBlockEntry;");
        Variable blockBuilder = scope.createTempVariable(BlockBuilder.class);
        Variable singleRowBlockWriter = scope.createTempVariable(BlockBuilder.class);
        block.append(blockBuilder.set(
                constantType(binder, rowType).invoke(
                        "createBlockBuilder",
                        BlockBuilder.class,
                        constantNull(BlockBuilderStatus.class),
                        constantInt(1))));
        block.append(singleRowBlockWriter.set(blockBuilder.invoke("beginBlockEntry", BlockBuilder.class)));

        // We optimize temp variables - one each for primitives and Object for all non-primitives
        Map<Type, Variable> varMap = new HashMap<>();
        Map<Type, LabelNode> labelMap = new HashMap<>();
        for (int i = 0; i < arguments.size(); ++i) {
            Type fieldType = types.get(i);
            Variable variable;
            if (!varMap.containsKey(fieldType)) {
                Class javaType = fieldType.getJavaType().isPrimitive() ? fieldType.getJavaType() : Object.class;
                variable = scope.getSingletonVariable(block, javaType);
                varMap.put(fieldType, variable);
                labelMap.put(fieldType, new LabelNode("store_" + i));
            }
        }

        block.append(context.wasNull().set(constantFalse()));
        Variable index = scope.createTempVariable(int.class);
        BytecodeBlock loopBody = new BytecodeBlock();
        BytecodeNode forLoop = new ForLoop()
                .initialize(new BytecodeBlock().putVariable(index, 0))
                .condition(new BytecodeBlock()
                        .getVariable(index)
                        .append(constantInt(arguments.size()))
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new BytecodeBlock().incrementVariable(index, (byte) 1))
                .body(loopBody);

        LabelNode endLoop = new LabelNode("endLoop");
        LabelNode addNull = new LabelNode("addNull");
        SwitchStatement.SwitchBuilder switchBuilder = new SwitchStatement.SwitchBuilder()
                .comment("switch(i) { case label_for_type_of_field_i: temp_of_type_i = eval(arg[i]); goto add_field_of_type_of_field_i ... }")
                .expression(index);
        for (int i = 0; i < arguments.size(); ++i) {
            Type fieldType = types.get(i);
            BytecodeBlock caseBlock = new BytecodeBlock();
            caseBlock.append(context.generate(arguments.get(i), Optional.empty()));
            caseBlock.putVariable(varMap.get(fieldType));
            caseBlock.gotoLabel(labelMap.get(fieldType));
            switchBuilder.addCase(i, caseBlock);
        }

        switchBuilder.defaultCase(jump(endLoop));
        loopBody.append(switchBuilder.build());
        for (Map.Entry<Type, LabelNode> labelsByType : labelMap.entrySet()) {
            Type fieldType = labelsByType.getKey();
            LabelNode labelNode = labelsByType.getValue();
            loopBody.visitLabel(labelNode).comment(" if (wasnull) goto addLull; add_field_of_type to singleRowBlockWriter; goto endLoop");
            loopBody.append(new IfStatement()
                        .condition(context.wasNull())
                        .ifTrue(jump(addNull))
                        .ifFalse(constantType(binder, fieldType).writeValue(singleRowBlockWriter, varMap.get(fieldType).cast(fieldType.getJavaType())).pop()));
            loopBody.gotoLabel(endLoop);
        }

        // Code to add null for the current field position.
        loopBody.visitLabel(addNull).comment("singleRowBlockWriter.addnull(i); wasNull = false;");
        loopBody.append(singleRowBlockWriter.invoke("appendNull", BlockBuilder.class))
                .pop()
                .append(context.wasNull().set(constantFalse()));

        loopBody.visitLabel(endLoop);
        block.append(forLoop);
        block.comment("closeEntry; slice the SingleRowBlock; wasNull = false;");
        block.append(blockBuilder.invoke("closeEntry", BlockBuilder.class).pop());
        block.append(constantType(binder, rowType).invoke("getObject", Object.class, blockBuilder.cast(Block.class), constantInt(0))
                .cast(Block.class));
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(context, rowType, output)));
        return block;
    }
}
