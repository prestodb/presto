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
package com.facebook.presto.byteCode.expression;

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.instruction.InstructionNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.ArrayOpCode.getArrayOpCode;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class SetArrayElementByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression instance;
    private final ByteCodeExpression index;
    private final ByteCodeExpression value;
    private final InstructionNode arrayStoreInstruction;

    public SetArrayElementByteCodeExpression(ByteCodeExpression instance, ByteCodeExpression index, ByteCodeExpression value)
    {
        super(type(void.class));

        this.instance = requireNonNull(instance, "instance is null");
        this.index = requireNonNull(index, "index is null");
        this.value = requireNonNull(value, "value is null");

        ParameterizedType componentType = instance.getType().getArrayComponentType();
        checkArgument(index.getType().getPrimitiveType() == int.class, "index must be int type, but is " + index.getType());
        checkArgument(componentType.equals(value.getType()), "value must be %s type, but is %s", componentType, value.getType());

        this.arrayStoreInstruction = getArrayOpCode(componentType).getStore();
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return new ByteCodeBlock()
                .append(instance.getByteCode(generationContext))
                .append(index)
                .append(value)
                .append(arrayStoreInstruction);
    }

    @Override
    protected String formatOneLine()
    {
        return instance + "[" + index + "] = " + value;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(index, value);
    }
}
