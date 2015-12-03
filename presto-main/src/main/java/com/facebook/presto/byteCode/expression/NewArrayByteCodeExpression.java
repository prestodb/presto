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
import com.facebook.presto.byteCode.instruction.TypeInstruction;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static java.util.Objects.requireNonNull;

class NewArrayByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression length;
    private final ParameterizedType type;

    public NewArrayByteCodeExpression(ParameterizedType type, int length)
    {
        this(type, constantInt(length));
    }

    public NewArrayByteCodeExpression(ParameterizedType type, ByteCodeExpression length)
    {
        super(type);
        this.type = requireNonNull(type, "type is null");
        this.length = requireNonNull(length, "length is null");
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        if (type.getArrayComponentType().isPrimitive()) {
            return new ByteCodeBlock()
                    .append(length)
                    .append(TypeInstruction.newPrimitiveArray(type.getArrayComponentType()));
        }
        else {
            return new ByteCodeBlock()
                    .append(length)
                    .append(TypeInstruction.newObjectArray(type.getArrayComponentType()));
        }
    }

    @Override
    protected String formatOneLine()
    {
        return "new " + getType().getArrayComponentType().getSimpleName() + "[" + length + "]";
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }
}
