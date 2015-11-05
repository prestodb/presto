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
import com.facebook.presto.byteCode.instruction.TypeInstruction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.OpCode.AALOAD;
import static com.facebook.presto.byteCode.OpCode.AASTORE;
import static com.facebook.presto.byteCode.OpCode.ARRAYLENGTH;
import static com.facebook.presto.byteCode.OpCode.BALOAD;
import static com.facebook.presto.byteCode.OpCode.BASTORE;
import static com.facebook.presto.byteCode.OpCode.CALOAD;
import static com.facebook.presto.byteCode.OpCode.CASTORE;
import static com.facebook.presto.byteCode.OpCode.DALOAD;
import static com.facebook.presto.byteCode.OpCode.DASTORE;
import static com.facebook.presto.byteCode.OpCode.FALOAD;
import static com.facebook.presto.byteCode.OpCode.FASTORE;
import static com.facebook.presto.byteCode.OpCode.IALOAD;
import static com.facebook.presto.byteCode.OpCode.IASTORE;
import static com.facebook.presto.byteCode.OpCode.LALOAD;
import static com.facebook.presto.byteCode.OpCode.LASTORE;
import static com.facebook.presto.byteCode.OpCode.SALOAD;
import static com.facebook.presto.byteCode.OpCode.SASTORE;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_BOOLEAN;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_BYTE;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_CHAR;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_DOUBLE;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_FLOAT;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_INT;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_LONG;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.AType.T_SHORT;
import static com.facebook.presto.byteCode.expression.ArrayByteCodeExpressions.ArrayOpCode.getArrayOpCode;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ArrayByteCodeExpressions
{
    private ArrayByteCodeExpressions() {}

    public static ByteCodeExpression newArray(ParameterizedType type, int length)
    {
        return new NewArray(type, length);
    }

    public static ByteCodeExpression newArray(ParameterizedType type, ByteCodeExpression length)
    {
        return new NewArray(type, length);
    }

    public static ByteCodeExpression length(ByteCodeExpression instance)
    {
        return new ArrayLength(instance);
    }

    public static ByteCodeExpression get(ByteCodeExpression instance, ByteCodeExpression index)
    {
        return new GetElement(instance, index);
    }

    public static ByteCodeExpression set(ByteCodeExpression instance, ByteCodeExpression index, ByteCodeExpression value)
    {
        return new SetArrayElement(instance, index, value);
    }

    private static class NewArray
            extends ByteCodeExpression
    {
        private final ByteCodeExpression length;
        private final ParameterizedType type;

        public NewArray(ParameterizedType type, int length)
        {
            this(type, constantInt(length));
        }

        public NewArray(ParameterizedType type, ByteCodeExpression length)
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
            return "new " + getType().getSimpleName() + "{length = " + length + "}";
        }

        @Override
        public List<ByteCodeNode> getChildNodes()
        {
            return ImmutableList.of();
        }
    }

    private static class ArrayLength
            extends ByteCodeExpression
    {
        private final ByteCodeExpression instance;

        public ArrayLength(ByteCodeExpression instance)
        {
            super(type(int.class));
            this.instance = requireNonNull(instance, "instance is null");
        }

        @Override
        public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
        {
            return new ByteCodeBlock()
                    .append(instance.getByteCode(generationContext))
                    .append(ARRAYLENGTH);
        }

        @Override
        protected String formatOneLine()
        {
            return instance + ".length";
        }

        @Override
        public List<ByteCodeNode> getChildNodes()
        {
            return ImmutableList.<ByteCodeNode>of();
        }
    }

    private static class SetArrayElement
            extends ByteCodeExpression
    {
        private final ByteCodeExpression instance;
        private final ByteCodeExpression index;
        private final ByteCodeExpression value;
        private final InstructionNode arrayStoreInstruction;

        public SetArrayElement(ByteCodeExpression instance, ByteCodeExpression index, ByteCodeExpression value)
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

    private static class GetElement
            extends ByteCodeExpression
    {
        private final ByteCodeExpression instance;
        private final ByteCodeExpression index;
        private final InstructionNode arrayLoadInstruction;

        public GetElement(ByteCodeExpression instance, ByteCodeExpression index)
        {
            super(instance.getType().getArrayComponentType());
            this.instance = requireNonNull(instance, "instance is null");
            this.index = requireNonNull(index, "index is null");

            checkArgument(index.getType().getPrimitiveType() == int.class, "index must be int type, but is " + index.getType());
            this.arrayLoadInstruction = getArrayOpCode(instance.getType().getArrayComponentType()).getLoad();
        }

        @Override
        public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
        {
            return new ByteCodeBlock()
                    .append(instance.getByteCode(generationContext))
                    .append(index)
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
            return ImmutableList.<ByteCodeNode>of(index);
        }
    }

    enum AType
    {
        T_BOOLEAN(4),
        T_CHAR(5),
        T_FLOAT(6),
        T_DOUBLE(7),
        T_BYTE(8),
        T_SHORT(9),
        T_INT(10),
        T_LONG(11);

        private final int type;

        AType(int type)
        {
            this.type = type;
        }

        int getType()
        {
            return type;
        }
    }

    public enum ArrayOpCode
    {
        NOT_PRIMITIVE(null, AALOAD, AASTORE, null),
        BYTE(byte.class, BALOAD, BASTORE, T_BYTE),
        BOOLEAN(boolean.class, BALOAD, BASTORE, T_BOOLEAN),
        CHAR(char.class, CALOAD, CASTORE, T_CHAR),
        SHORT(short.class, SALOAD, SASTORE, T_SHORT),
        INT(int.class, IALOAD, IASTORE, T_INT),
        LONG(long.class, LALOAD, LASTORE, T_LONG),
        FLOAT(float.class, FALOAD, FASTORE, T_FLOAT),
        DOUBLE(double.class, DALOAD, DASTORE, T_DOUBLE);

        private final OpCode load;
        private final OpCode store;
        private final AType atype;
        private final Class<?> type;

        private static final Map<Class<?>, ArrayOpCode> arrayOpCodeMap = initializeArrayOpCodeMap();

        ArrayOpCode(@Nullable Class<?> clazz, OpCode load, OpCode store, @Nullable AType atype)
        {
            this.type = clazz;
            this.load = requireNonNull(load, "load is null");
            this.store = requireNonNull(store, "store is null");
            this.atype = atype;
        }

        public OpCode getLoad()
        {
            return load;
        }

        public OpCode getStore()
        {
            return store;
        }

        public int getAtype()
        {
            return atype.getType();
        }

        public Class<?> getType()
        {
            return type;
        }

        public static ArrayOpCode getArrayOpCode(ParameterizedType type)
        {
            if (!type.isPrimitive()) {
                return NOT_PRIMITIVE;
            }
            ArrayOpCode arrayOpCode = arrayOpCodeMap.get(type.getPrimitiveType());
            if (arrayOpCode == null) {
                throw new IllegalArgumentException("unsupported primitive type " + type);
            }
            return arrayOpCode;
        }

        static Map<Class<?>, ArrayOpCode> initializeArrayOpCodeMap()
        {
            ImmutableMap.Builder<Class<?>, ArrayOpCode> builder = ImmutableMap.<Class<?>, ArrayOpCode>builder();
            for (ArrayOpCode arrayOpCode : values()) {
                if (arrayOpCode.getType() != null) {
                    builder.put(arrayOpCode.getType(), arrayOpCode);
                }
            }
            return builder.build();
        }
    }
}
