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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

class ReturnByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression instance;
    private final OpCode returnOpCode;

    ReturnByteCodeExpression(ByteCodeExpression instance)
    {
        super(type(void.class));
        this.instance = requireNonNull(instance, "instance is null");
        this.returnOpCode = returnOpCode(instance.getType());
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return new ByteCodeBlock()
                .append(instance.getByteCode(generationContext))
                .append(returnOpCode);
    }

    @Override
    protected String formatOneLine()
    {
        return "return " + instance;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(instance);
    }

    private static OpCode returnOpCode(ParameterizedType componentType)
    {
        Class<?> primitiveType = componentType.getPrimitiveType();
        if (primitiveType != null) {
            if (primitiveType == byte.class ||
                    primitiveType == boolean.class ||
                    primitiveType == char.class ||
                    primitiveType == short.class ||
                    primitiveType == int.class) {
                return OpCode.IRETURN;
            }
            if (primitiveType == long.class) {
                return OpCode.LRETURN;
            }
            if (primitiveType == float.class) {
                return OpCode.FRETURN;
            }
            if (primitiveType == double.class) {
                return OpCode.DRETURN;
            }
            if (primitiveType == void.class) {
                return OpCode.RETURN;
            }
            throw new IllegalArgumentException("Unsupported array type: " + primitiveType);
        }
        else {
            return OpCode.ARETURN;
        }
    }
}
