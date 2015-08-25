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
import com.facebook.presto.byteCode.OpCode;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.instruction.InstructionNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class GetElementByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression instance;
    private final ByteCodeExpression index;
    private final InstructionNode arrayLoadInstruction;

    public GetElementByteCodeExpression(ByteCodeExpression instance, ByteCodeExpression index)
    {
        super(instance.getType().getArrayComponentType());
        this.instance = checkNotNull(instance, "instance is null");
        this.index = checkNotNull(index, "index is null");
        checkArgument(index.getType().getPrimitiveType() == int.class, "index must be int type, but is " + index.getType());
        this.arrayLoadInstruction = arrayLoadInstruction(instance.getType().getArrayComponentType());
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return new ByteCodeBlock()
                .append(instance.getByteCode(generationContext)).append(index)
                .append(arrayLoadInstruction);
    }

    @Override
    protected String formatOneLine()
    {
        return instance + "[" + index + "]";
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(instance, index);
    }

    private static InstructionNode arrayLoadInstruction(ParameterizedType componentType)
    {
        Class<?> primitiveType = componentType.getPrimitiveType();
        if (primitiveType == null) {
            return OpCode.AALOAD;
        }

        if (primitiveType == byte.class || primitiveType == boolean.class) {
            return OpCode.BALOAD;
        }
        if (primitiveType == char.class) {
            return OpCode.CALOAD;
        }
        if (primitiveType == short.class) {
            return OpCode.SALOAD;
        }
        if (primitiveType == int.class) {
            return OpCode.IALOAD;
        }
        if (primitiveType == long.class) {
            return OpCode.LALOAD;
        }
        if (primitiveType == float.class) {
            return OpCode.FALOAD;
        }
        if (primitiveType == double.class) {
            return OpCode.DALOAD;
        }
        throw new IllegalArgumentException("Unsupported array type: " + primitiveType);
    }
}
