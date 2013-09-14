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
package com.facebook.presto.byteCode.instruction;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.ACONST_NULL;
import static com.facebook.presto.byteCode.OpCodes.BIPUSH;
import static com.facebook.presto.byteCode.OpCodes.DCONST_0;
import static com.facebook.presto.byteCode.OpCodes.DCONST_1;
import static com.facebook.presto.byteCode.OpCodes.FCONST_0;
import static com.facebook.presto.byteCode.OpCodes.FCONST_1;
import static com.facebook.presto.byteCode.OpCodes.FCONST_2;
import static com.facebook.presto.byteCode.OpCodes.ICONST_0;
import static com.facebook.presto.byteCode.OpCodes.ICONST_1;
import static com.facebook.presto.byteCode.OpCodes.ICONST_2;
import static com.facebook.presto.byteCode.OpCodes.ICONST_3;
import static com.facebook.presto.byteCode.OpCodes.ICONST_4;
import static com.facebook.presto.byteCode.OpCodes.ICONST_5;
import static com.facebook.presto.byteCode.OpCodes.ICONST_M1;
import static com.facebook.presto.byteCode.OpCodes.LCONST_0;
import static com.facebook.presto.byteCode.OpCodes.LCONST_1;
import static com.facebook.presto.byteCode.OpCodes.SIPUSH;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.instruction.FieldInstruction.getStaticInstruction;
import static com.facebook.presto.byteCode.instruction.InvokeInstruction.invokeStatic;

public abstract class Constant
        implements InstructionNode
{
    public static InstructionNode loadNull()
    {
        return ACONST_NULL;
    }

    public static InstructionNode loadBoolean(boolean value)
    {
        return new IntConstant(value ? 1 : 0);
    }

    public static InstructionNode loadBoxedBoolean(boolean value)
    {
        return new BoxedBooleanConstant(value);
    }

    public static InstructionNode loadInt(int value)
    {
        return new IntConstant(value);
    }

    public static InstructionNode loadBoxedInt(int value)
    {
        return new BoxedIntegerConstant(value);
    }

    public static InstructionNode loadFloat(float value)
    {
        return new FloatConstant(value);
    }

    public static InstructionNode loadBoxedFloat(float value)
    {
        return new BoxedFloatConstant(value);
    }

    public static InstructionNode loadLong(long value)
    {
        return new LongConstant(value);
    }

    public static InstructionNode loadBoxedLong(long value)
    {
        return new BoxedLongConstant(value);
    }

    public static InstructionNode loadDouble(double value)
    {
        return new DoubleConstant(value);
    }

    public static InstructionNode loadBoxedDouble(double value)
    {
        return new BoxedDoubleConstant(value);
    }

    public static InstructionNode loadNumber(Number value)
    {
        Preconditions.checkNotNull(value, "value is null");
        if (value instanceof Byte) {
            return loadInt((value).intValue());
        }
        if (value instanceof Short) {
            return loadInt((value).intValue());
        }
        if (value instanceof Integer) {
            return loadInt((Integer) value);
        }
        if (value instanceof Long) {
            return loadLong((Long) value);
        }
        if (value instanceof Float) {
            return loadFloat((Float) value);
        }
        if (value instanceof Double) {
            return loadDouble((Double) value);
        }
        throw new IllegalStateException("Unsupported number type " + value.getClass().getSimpleName());
    }

    public static InstructionNode loadString(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        return new StringConstant(value);
    }

    public static InstructionNode loadClass(Class<?> value)
    {
        Preconditions.checkNotNull(value, "value is null");
        return new ClassConstant(type(value));
    }

    public static InstructionNode loadClass(ParameterizedType value)
    {
        Preconditions.checkNotNull(value, "value is null");
        return new ClassConstant(value);
    }

    public abstract Object getValue();

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitConstant(parent, this);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", getValue())
                .toString();
    }

    public static class BoxedBooleanConstant
            extends Constant
    {
        private final boolean value;

        private BoxedBooleanConstant(boolean value)
        {
            this.value = value;
        }

        @Override
        public Boolean getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            if (value) {
                getStaticInstruction(Boolean.class, "TRUE", Boolean.class).accept(visitor);
            }
            else {
                getStaticInstruction(Boolean.class, "FALSE", Boolean.class).accept(visitor);
            }
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitBoxedBooleanConstant(parent, this);
        }
    }

    public static class IntConstant
            extends Constant
    {
        private final int value;

        private IntConstant(int value)
        {
            this.value = value;
        }

        @Override
        public Integer getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
                switch (value) {
                    case -1:
                        visitor.visitInsn(ICONST_M1.getOpCode());
                        break;
                    case 0:
                        visitor.visitInsn(ICONST_0.getOpCode());
                        break;
                    case 1:
                        visitor.visitInsn(ICONST_1.getOpCode());
                        break;
                    case 2:
                        visitor.visitInsn(ICONST_2.getOpCode());
                        break;
                    case 3:
                        visitor.visitInsn(ICONST_3.getOpCode());
                        break;
                    case 4:
                        visitor.visitInsn(ICONST_4.getOpCode());
                        break;
                    case 5:
                        visitor.visitInsn(ICONST_5.getOpCode());
                        break;
                    default:
                        visitor.visitIntInsn(BIPUSH.getOpCode(), value);
                        break;
                }
            }
            else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
                visitor.visitIntInsn(SIPUSH.getOpCode(), value);
            }
            else {
                visitor.visitLdcInsn(value);
            }
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitIntConstant(parent, this);
        }
    }

    public static class BoxedIntegerConstant
            extends Constant
    {
        private final int value;

        private BoxedIntegerConstant(int value)
        {
            this.value = value;
        }

        @Override
        public Integer getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            loadInt(value).accept(visitor);
            invokeStatic(Integer.class, "valueOf", Integer.class, int.class).accept(visitor);
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitBoxedIntegerConstant(parent, this);
        }
    }

    public static class FloatConstant
            extends Constant
    {
        private final float value;

        private FloatConstant(float value)
        {
            this.value = value;
        }

        @Override
        public Float getValue()
        {
            return value;
        }

        @Override
        @SuppressWarnings("FloatingPointEquality")
        public void accept(MethodVisitor visitor)
        {
            // We can not use "value == 0.0" because when value is "-0.0" the expression
            // will evaluate to true and we would convert "-0.0" to "0.0" which is
            // not the same value
            if (Float.floatToIntBits(value) == Float.floatToIntBits(0.0f)) {
                visitor.visitInsn(FCONST_0.getOpCode());
            }
            else if (value == 1.0f) {
                visitor.visitInsn(FCONST_1.getOpCode());
            }
            else if (value == 2.0f) {
                visitor.visitInsn(FCONST_2.getOpCode());
            }
            else {
                visitor.visitLdcInsn(value);
            }
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitFloatConstant(parent, this);
        }
    }

    public static class BoxedFloatConstant
            extends Constant
    {
        private final float value;

        private BoxedFloatConstant(float value)
        {
            this.value = value;
        }

        @Override
        public Float getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            loadFloat(value).accept(visitor);
            invokeStatic(Float.class, "valueOf", Float.class, float.class).accept(visitor);
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitBoxedFloatConstant(parent, this);
        }
    }

    public static class LongConstant
            extends Constant
    {
        private final long value;

        private LongConstant(long value)
        {
            this.value = value;
        }

        @Override
        public Long getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            if (value == 0) {
                visitor.visitInsn(LCONST_0.getOpCode());
            }
            else if (value == 1) {
                visitor.visitInsn(LCONST_1.getOpCode());
            }
            else {
                visitor.visitLdcInsn(value);
            }
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitLongConstant(parent, this);
        }
    }

    public static class BoxedLongConstant
            extends Constant
    {
        private final long value;

        private BoxedLongConstant(long value)
        {
            this.value = value;
        }

        @Override
        public Long getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            loadLong(value).accept(visitor);
            invokeStatic(Long.class, "valueOf", Long.class, long.class).accept(visitor);
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitBoxedLongConstant(parent, this);
        }
    }

    public static class DoubleConstant
            extends Constant
    {
        private final double value;

        private DoubleConstant(double value)
        {
            this.value = value;
        }

        @Override
        public Double getValue()
        {
            return value;
        }

        @Override
        @SuppressWarnings("FloatingPointEquality")
        public void accept(MethodVisitor visitor)
        {
            // We can not use "value == 0.0" because when value is "-0.0" the expression
            // will evaluate to true and we would convert "-0.0" to "0.0" which is
            // not the same value
            if (Double.doubleToLongBits(value) == Double.doubleToLongBits(0.0)) {
                visitor.visitInsn(DCONST_0.getOpCode());
            }
            else if (value == 1.0) {
                visitor.visitInsn(DCONST_1.getOpCode());
            }
            else {
                visitor.visitLdcInsn(value);
            }
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitDoubleConstant(parent, this);
        }
    }

    public static class BoxedDoubleConstant
            extends Constant
    {
        private final double value;

        private BoxedDoubleConstant(double value)
        {
            this.value = value;
        }

        @Override
        public Double getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            loadDouble(value).accept(visitor);
            invokeStatic(Double.class, "valueOf", Double.class, double.class).accept(visitor);
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitBoxedDoubleConstant(parent, this);
        }
    }

    public static class StringConstant
            extends Constant
    {
        private final String value;

        private StringConstant(String value)
        {
            this.value = value;
        }

        @Override
        public String getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            visitor.visitLdcInsn(value);
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitStringConstant(parent, this);
        }
    }

    public static class ClassConstant
            extends Constant
    {
        private final ParameterizedType value;

        private ClassConstant(ParameterizedType value)
        {
            this.value = value;
        }

        @Override
        public ParameterizedType getValue()
        {
            return value;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            visitor.visitLdcInsn(Type.getType(value.getType()));
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitClassConstant(parent, this);
        }
    }
}
