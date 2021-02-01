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
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UnknownType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.instruction.VariableInstruction.incrementVariable;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;

public class ArrayMapBytecodeExpression
        extends BytecodeExpression
{
    private static final AtomicLong NEXT_VARIABLE_ID = new AtomicLong();

    private final BytecodeBlock body;
    private final String oneLineDescription;

    public ArrayMapBytecodeExpression(
            Scope scope,
            CallSiteBinder binder,
            BytecodeExpression array,
            Type fromType,
            Type toType,
            Function<BytecodeExpression, BytecodeExpression> mapper)
    {
        super(type(Block.class));

        body = new BytecodeBlock();

        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder_" + NEXT_VARIABLE_ID.getAndIncrement());
        body.append(blockBuilder.set(constantType(binder, toType).invoke("createBlockBuilder", BlockBuilder.class, constantNull(BlockBuilderStatus.class), array.invoke("getPositionCount", int.class))));

        // get element, apply function, and write new element to block builder
        Variable position = scope.declareVariable(int.class, "position_" + NEXT_VARIABLE_ID.getAndIncrement());
        BytecodeBlock mapElement;
        String mapperDescription;
        if (fromType instanceof UnknownType) {
            mapElement = new BytecodeBlock()
                    .comment("unreachable code");
            mapperDescription = "null";
        }
        else {
            Variable element = scope.declareVariable(fromType.getJavaType(), "element_" + NEXT_VARIABLE_ID.getAndIncrement());
            Variable newElement = scope.declareVariable(toType.getJavaType(), "newElement_" + NEXT_VARIABLE_ID.getAndIncrement());
            SqlTypeBytecodeExpression elementTypeConstant = constantType(binder, fromType);
            SqlTypeBytecodeExpression newElementTypeConstant = constantType(binder, toType);
            mapElement = new BytecodeBlock()
                    .append(element.set(elementTypeConstant.getValue(array, position)))
                    .append(newElement.set(mapper.apply(element)))
                    .append(newElementTypeConstant.writeValue(blockBuilder, newElement));
            mapperDescription = mapper.apply(element).toString();
        }

        // main loop
        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, array.invoke("getPositionCount", int.class)))
                .update(incrementVariable(position, (byte) 1))
                .body(new IfStatement()
                        .condition(array.invoke("isNull", boolean.class, position))
                        .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                        .ifFalse(mapElement)));

        // build block
        body.append(blockBuilder.invoke("build", Block.class));

        // pretty print
        oneLineDescription = "arrayMap(" + array + ", element -> " + mapperDescription + ")";
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return body;
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return oneLineDescription;
    }
}
