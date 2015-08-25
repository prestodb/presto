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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.lessThan;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newInstance;
import static com.facebook.presto.byteCode.instruction.VariableInstruction.incrementVariable;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;

public class ArrayMapByteCodeExpression
        extends ByteCodeExpression
{
    private static final AtomicLong NEXT_VARIABLE_ID = new AtomicLong();

    private final ByteCodeBlock body;
    private final String oneLineDescription;

    public ArrayMapByteCodeExpression(
            Scope scope,
            CallSiteBinder binder,
            ByteCodeExpression array,
            Type fromType,
            Type toType,
            Function<ByteCodeExpression, ByteCodeExpression> mapper)
    {
        super(type(Block.class));

        body = new ByteCodeBlock();

        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder_" + NEXT_VARIABLE_ID.getAndIncrement());
        body.append(blockBuilder.set(constantType(binder, toType).invoke("createBlockBuilder", BlockBuilder.class, newInstance(BlockBuilderStatus.class), array.invoke("getPositionCount", int.class))));

        Variable element = scope.declareVariable(fromType.getJavaType(), "element_" + NEXT_VARIABLE_ID.getAndIncrement());
        Variable newElement = scope.declareVariable(toType.getJavaType(), "newElement_" + NEXT_VARIABLE_ID.getAndIncrement());
        Variable position = scope.declareVariable(int.class, "position_" + NEXT_VARIABLE_ID.getAndIncrement());

        // get element, apply function, and write new element to block builder
        SqlTypeByteCodeExpression elementTypeConstant = constantType(binder, fromType);
        SqlTypeByteCodeExpression newElementTypeConstant = constantType(binder, toType);
        ByteCodeBlock mapElement = new ByteCodeBlock()
                .append(element.set(elementTypeConstant.getValue(array, position)))
                .append(newElement.set(mapper.apply(element)))
                .append(newElementTypeConstant.writeValue(blockBuilder, newElement));

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
        oneLineDescription = "arrayMap(" + array + ", element -> " + mapper.apply(element) + ")";
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return body;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return oneLineDescription;
    }
}
