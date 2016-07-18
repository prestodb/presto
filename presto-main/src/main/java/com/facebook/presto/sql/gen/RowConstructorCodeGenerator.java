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
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;

import java.util.List;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;

public class RowConstructorCodeGenerator
        implements BytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext context, Type rowType, List<RowExpression> arguments)
    {
        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType.toString());
        CallSiteBinder binder = context.getCallSiteBinder();
        Scope scope = context.getScope();
        List<Type> types = rowType.getTypeParameters();

        block.comment("BlockBuilder blockBuilder = new InterleavedBlockBuilder(types, new BlockBuilderStatus(), 1);");
        Variable blockBuilder = scope.createTempVariable(BlockBuilder.class);
        Binding typesBinding = binder.bind(types, List.class);
        block.append(blockBuilder.set(
                newInstance(InterleavedBlockBuilder.class, loadConstant(typesBinding), newInstance(BlockBuilderStatus.class), constantInt(1))));

        for (int i = 0; i < arguments.size(); ++i) {
            Type fieldType = types.get(i);
            Class<?> javaType = fieldType.getJavaType();
            if (javaType == void.class) {
                block.comment(i + "-th field type of row is undefined");
                block.append(blockBuilder.invoke("appendNull", BlockBuilder.class).pop());
            }
            else {
                Variable field = scope.createTempVariable(javaType);
                block.comment("Clean wasNull and Generate + " + i + "-th field of row");
                block.append(context.wasNull().set(constantFalse()));
                block.append(context.generate(arguments.get(i)));
                block.putVariable(field);
                block.append(new IfStatement()
                        .condition(context.wasNull())
                        .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                        .ifFalse(constantType(binder, fieldType).writeValue(blockBuilder, field).pop()));
            }
        }
        block.comment("put (Block) blockBuilder.build(); wasNull = false;");
        block.append(blockBuilder.invoke("build", Block.class));
        block.append(context.wasNull().set(constantFalse()));
        return block;
    }
}
