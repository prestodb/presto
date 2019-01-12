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
package io.prestosql.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.MethodGenerationContext;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.type.Type;
import io.prestosql.type.UnknownType;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.instruction.VariableInstruction.incrementVariable;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;

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
