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
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.OpCode;
import com.facebook.presto.bytecode.ParameterizedType;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Primitives.wrap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class CastBytecodeExpression
        extends BytecodeExpression
{
    private static final ParameterizedType OBJECT_TYPE = type(Object.class);

    private final BytecodeExpression instance;

    public CastBytecodeExpression(BytecodeExpression instance, ParameterizedType type)
    {
        super(type);

        this.instance = requireNonNull(instance, "instance is null");

        checkArgument(type.getPrimitiveType() != void.class, "Type %s can not be cast to %s", instance.getType(), type);

        // Call generateBytecode to run the validation logic. The result is thrown away.
        // Duplicating the validation logic here is error-prone and introduces duplicate code.
        generateBytecode(instance.getType(), getType());
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return new BytecodeBlock()
                .append(instance.getBytecode(generationContext))
                .append(generateBytecode(instance.getType(), getType()));
    }

    private static BytecodeBlock generateBytecode(ParameterizedType sourceType, ParameterizedType targetType)
    {
        BytecodeBlock block = new BytecodeBlock();

        switch (getTypeKind(sourceType)) {
            case PRIMITIVE:
                switch (getTypeKind(targetType)) {
                    case PRIMITIVE:
                        castPrimitiveToPrimitive(block, sourceType.getPrimitiveType(), targetType.getPrimitiveType());
                        return block;
                    case BOXED_PRIMITVE:
                        checkArgument(sourceType.getPrimitiveType() == unwrapPrimitiveType(targetType), "Type %s can not be cast to %s", sourceType, targetType);
                        return block.invokeStatic(targetType, "valueOf", targetType, sourceType);
                    case OTHER:
                        checkArgument(OBJECT_TYPE.equals(targetType), "Type %s can not be cast to %s", sourceType, targetType);
                        Class<?> sourceClass = sourceType.getPrimitiveType();
                        return block
                                .invokeStatic(wrap(sourceClass), "valueOf", wrap(sourceClass), sourceClass)
                                .checkCast(targetType);
                }
            case BOXED_PRIMITVE:
                switch (getTypeKind(targetType)) {
                    case PRIMITIVE:
                        checkArgument(unwrapPrimitiveType(sourceType) == targetType.getPrimitiveType(), "Type %s can not be cast to %s", sourceType, targetType);
                        return block.invokeVirtual(sourceType, targetType.getPrimitiveType().getSimpleName() + "Value", targetType);
                    case BOXED_PRIMITVE:
                        checkArgument(sourceType.equals(targetType), "Type %s can not be cast to %s", sourceType, targetType);
                        return block;
                    case OTHER:
                        return block.checkCast(targetType);
                }
            case OTHER:
                switch (getTypeKind(targetType)) {
                    case PRIMITIVE:
                        checkArgument(OBJECT_TYPE.equals(sourceType), "Type %s can not be cast to %s", sourceType, targetType);
                        return block
                                .checkCast(wrap(targetType.getPrimitiveType()))
                                .invokeVirtual(wrap(targetType.getPrimitiveType()), targetType.getPrimitiveType().getSimpleName() + "Value", targetType.getPrimitiveType());
                    case BOXED_PRIMITVE:
                    case OTHER:
                        return block.checkCast(targetType);
                }
        }
        throw new UnsupportedOperationException("unexpected enum value");
    }

    private static BytecodeBlock castPrimitiveToPrimitive(BytecodeBlock block, Class<?> sourceType, Class<?> targetType)
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

    private static TypeKind getTypeKind(ParameterizedType type)
    {
        if (type.isPrimitive()) {
            return TypeKind.PRIMITIVE;
        }
        if (unwrapPrimitiveType(type) != null) {
            return TypeKind.BOXED_PRIMITVE;
        }
        return TypeKind.OTHER;
    }

    private static Class<?> unwrapPrimitiveType(ParameterizedType boxedPrimitiveType)
    {
        switch (boxedPrimitiveType.getJavaClassName()) {
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
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.<BytecodeNode>of(instance);
    }

    private enum TypeKind {
        PRIMITIVE, BOXED_PRIMITVE, OTHER
    }
}
