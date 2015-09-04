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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Primitives.wrap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class CastByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression instance;

    public CastByteCodeExpression(ByteCodeExpression instance, ParameterizedType type)
    {
        super(type);

        this.instance = requireNonNull(instance, "instance is null");

        checkArgument(type.getPrimitiveType() != void.class, "Type %s can not be cast to %s", instance.getType(), type);

        // if we have a primitive to object or object to primitive conversion, it must be an exact boxing or unboxing conversion
        if (instance.getType().isPrimitive() != type.isPrimitive()) {
            checkArgument(unwrapPrimitiveType(instance.getType()) == unwrapPrimitiveType(type), "Type %s can not be cast to %s", instance.getType(), type);
        }
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        ByteCodeBlock block = new ByteCodeBlock().append(instance.getByteCode(generationContext));

        if (instance.getType().isPrimitive()) {
            Class<?> sourceType = instance.getType().getPrimitiveType();
            castPrimitiveToPrimitive(block, sourceType, unwrapPrimitiveType(getType()));

            // insert boxing conversion
            if (!getType().isPrimitive()) {
                Class<?> primitiveTargetType = unwrapPrimitiveType(getType());
                return block.invokeStatic(getType(), "valueOf", getType(), type(primitiveTargetType));
            }

            return block;
        }
        else if (getType().isPrimitive()) {
            // unbox
            Class<?> targetType = getType().getPrimitiveType();
            return block.invokeVirtual(wrap(targetType), targetType.getSimpleName() + "Value", targetType);
        }
        else {
            block.checkCast(getType());
        }
        return block;
    }

    private static ByteCodeBlock castPrimitiveToPrimitive(ByteCodeBlock block, Class<?> sourceType, Class<?> targetType)
    {
        if (sourceType == boolean.class) {
            if (targetType == boolean.class) {
                return block;
            }
        }
        if (sourceType == byte.class) {
            if (targetType == byte.class) {
                return block;
            }
            if (targetType == char.class) {
                return block;
            }
            if (targetType == short.class) {
                return block;
            }
            if (targetType == int.class) {
                return block;
            }
            if (targetType == long.class) {
                return block.append(OpCode.I2L);
            }
            if (targetType == float.class) {
                return block.append(OpCode.I2F);
            }
            if (targetType == double.class) {
                return block.append(OpCode.I2D);
            }
        }
        if (sourceType == char.class) {
            if (targetType == byte.class) {
                return block.append(OpCode.I2B);
            }
            if (targetType == char.class) {
                return block;
            }
            if (targetType == short.class) {
                return block;
            }
            if (targetType == int.class) {
                return block;
            }
            if (targetType == long.class) {
                return block.append(OpCode.I2L);
            }
            if (targetType == float.class) {
                return block.append(OpCode.I2F);
            }
            if (targetType == double.class) {
                return block.append(OpCode.I2D);
            }
        }
        if (sourceType == short.class) {
            if (targetType == byte.class) {
                return block.append(OpCode.I2B);
            }
            if (targetType == char.class) {
                return block.append(OpCode.I2C);
            }
            if (targetType == short.class) {
                return block;
            }
            if (targetType == int.class) {
                return block;
            }
            if (targetType == long.class) {
                return block.append(OpCode.I2L);
            }
            if (targetType == float.class) {
                return block.append(OpCode.I2F);
            }
            if (targetType == double.class) {
                return block.append(OpCode.I2D);
            }
        }
        if (sourceType == int.class) {
            if (targetType == boolean.class) {
                return block;
            }
            if (targetType == byte.class) {
                return block.append(OpCode.I2B);
            }
            if (targetType == char.class) {
                return block.append(OpCode.I2C);
            }
            if (targetType == short.class) {
                return block.append(OpCode.I2S);
            }
            if (targetType == int.class) {
                return block;
            }
            if (targetType == long.class) {
                return block.append(OpCode.I2L);
            }
            if (targetType == float.class) {
                return block.append(OpCode.I2F);
            }
            if (targetType == double.class) {
                return block.append(OpCode.I2D);
            }
        }
        if (sourceType == long.class) {
            if (targetType == byte.class) {
                return block.append(OpCode.L2I).append(OpCode.I2B);
            }
            if (targetType == char.class) {
                return block.append(OpCode.L2I).append(OpCode.I2C);
            }
            if (targetType == short.class) {
                return block.append(OpCode.L2I).append(OpCode.I2S);
            }
            if (targetType == int.class) {
                return block.append(OpCode.L2I);
            }
            if (targetType == long.class) {
                return block;
            }
            if (targetType == float.class) {
                return block.append(OpCode.L2F);
            }
            if (targetType == double.class) {
                return block.append(OpCode.L2D);
            }
        }
        if (sourceType == float.class) {
            if (targetType == byte.class) {
                return block.append(OpCode.F2I).append(OpCode.I2B);
            }
            if (targetType == char.class) {
                return block.append(OpCode.F2I).append(OpCode.I2C);
            }
            if (targetType == short.class) {
                return block.append(OpCode.F2I).append(OpCode.I2S);
            }
            if (targetType == int.class) {
                return block.append(OpCode.F2I);
            }
            if (targetType == long.class) {
                return block.append(OpCode.F2L);
            }
            if (targetType == float.class) {
                return block;
            }
            if (targetType == double.class) {
                return block.append(OpCode.F2D);
            }
        }
        if (sourceType == double.class) {
            if (targetType == byte.class) {
                return block.append(OpCode.D2I).append(OpCode.I2B);
            }
            if (targetType == char.class) {
                return block.append(OpCode.D2I).append(OpCode.I2C);
            }
            if (targetType == short.class) {
                return block.append(OpCode.D2I).append(OpCode.I2S);
            }
            if (targetType == int.class) {
                return block.append(OpCode.D2I);
            }
            if (targetType == long.class) {
                return block.append(OpCode.D2L);
            }
            if (targetType == float.class) {
                return block.append(OpCode.D2F);
            }
            if (targetType == double.class) {
                return block;
            }
        }
        throw new IllegalArgumentException(format("Type %s can not be cast to %s", sourceType, targetType));
    }

    private static Class<?> unwrapPrimitiveType(ParameterizedType type)
    {
        if (type.isPrimitive()) {
            return type.getPrimitiveType();
        }
        switch (type.getJavaClassName()) {
            case "java.lang.Boolean":
                return boolean.class;
            case "java.lang.Byte":
                return byte.class;
            case "java.lang.Character":
                return char.class;
            case "java.lang.Short":
                return short.class;
            case "java.lang.Integer":
                return int.class;
            case "java.lang.Long":
                return long.class;
            case "java.lang.Float":
                return float.class;
            case "java.lang.Double":
                return double.class;
            default:
                return null;
        }
    }

    @Override
    protected String formatOneLine()
    {
        return "((" + getType().getSimpleName() + ") " + instance + ")";
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(instance);
    }
}
