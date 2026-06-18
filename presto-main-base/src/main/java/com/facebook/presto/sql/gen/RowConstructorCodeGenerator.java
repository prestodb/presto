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
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
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

        for (int i = 0; i < arguments.size(); ++i) {
            Type fieldType = types.get(i);
            Variable field = scope.createTempVariable(fieldType.getJavaType());
            block.comment("Clean wasNull and Generate + " + i + "-th field of row");
            block.append(context.wasNull().set(constantFalse()));
            block.append(context.generate(arguments.get(i), Optional.empty()));
            block.putVariable(field);
            block.append(new IfStatement()
                    .condition(context.wasNull())
                    .ifTrue(singleRowBlockWriter.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, fieldType).writeValue(singleRowBlockWriter, field).pop()));
        }
        block.comment("closeEntry; slice the SingleRowBlock; wasNull = false;");
        block.append(blockBuilder.invoke("closeEntry", BlockBuilder.class).pop());
        block.append(constantType(binder, rowType).invoke("getObject", Object.class, blockBuilder.cast(Block.class), constantInt(0))
                .cast(Block.class));
        block.append(context.wasNull().set(constantFalse()));
        outputBlockVariable.ifPresent(output -> block.append(generateWrite(context, rowType, output)));
        return block;
    }
}
