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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.RowType;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionRegistry.mangleFieldAccessor;
import static com.facebook.presto.spi.block.StructBuilder.readBlock;
import static com.facebook.presto.type.RowType.RowField;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RowFieldAccessor
        extends ParametricScalar
{
    private static final Map<String, MethodHandle> METHOD_HANDLE_MAP;

    private final Signature signature;
    private final MethodHandle methodHandle;

    static {
        ImmutableMap.Builder<String, MethodHandle> builder = ImmutableMap.builder();
        builder.put("long", Reflection.methodHandle(RowFieldAccessor.class, "longAccessor", FieldExtractor.class, Slice.class));
        builder.put("double", Reflection.methodHandle(RowFieldAccessor.class, "doubleAccessor", FieldExtractor.class, Slice.class));
        builder.put("boolean", Reflection.methodHandle(RowFieldAccessor.class, "booleanAccessor", FieldExtractor.class, Slice.class));
        builder.put("slice", Reflection.methodHandle(RowFieldAccessor.class, "sliceAccessor", FieldExtractor.class, Slice.class));
        METHOD_HANDLE_MAP = builder.build();
    }

    public RowFieldAccessor(RowType type, String fieldName)
    {
        Type returnType = null;
        int index = 0;
        for (RowField field : type.getFields()) {
            if (field.getName().equals(Optional.of(fieldName))) {
                returnType = field.getType();
                break;
            }
            index++;
        }
        checkNotNull(returnType, "%s not found in row type %s", fieldName, type);
        signature = new Signature(mangleFieldAccessor(fieldName), returnType.getTypeSignature(), type.getTypeSignature());

        FieldExtractor<?> extractor;
        if (returnType.getJavaType() == boolean.class) {
            extractor = new BooleanFieldExtractor(returnType, index);
        }
        else if (returnType.getJavaType() == long.class) {
            extractor = new LongFieldExtractor(returnType, index);
        }
        else if (returnType.getJavaType() == double.class) {
            extractor = new DoubleFieldExtractor(returnType, index);
        }
        else if (returnType.getJavaType() == Slice.class) {
            extractor = new SliceFieldExtractor(returnType, index);
        }
        else {
            throw new IllegalArgumentException("Unsupported stack type: " + returnType.getJavaType());
        }
        String stackType = returnType.getJavaType().getSimpleName().toLowerCase();
        checkState(METHOD_HANDLE_MAP.containsKey(stackType), "method handle missing for %s stack type", stackType);
        methodHandle = METHOD_HANDLE_MAP.get(stackType).bindTo(extractor);
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkNotNull(methodHandle, "methodHandle is null");
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), true, ImmutableList.of(false));
    }

    public static Long longAccessor(FieldExtractor<Long> extractor, Slice row)
    {
        return extractor.extract(row);
    }

    public static Boolean booleanAccessor(FieldExtractor<Boolean> extractor, Slice row)
    {
        return extractor.extract(row);
    }

    public static Double doubleAccessor(FieldExtractor<Double> extractor, Slice row)
    {
        return extractor.extract(row);
    }

    public static Slice sliceAccessor(FieldExtractor<Slice> extractor, Slice row)
    {
        return extractor.extract(row);
    }

    private abstract class FieldExtractor<T>
    {
        protected final Type type;
        protected final int index;

        public FieldExtractor(Type type, int index)
        {
            this.type = type;
            this.index = index;
        }

        public abstract T extract(Slice row);
    }

    private class BooleanFieldExtractor extends FieldExtractor<Boolean>
    {
        public BooleanFieldExtractor(Type type, int index)
        {
            super(type, index);
        }

        @Override
        public Boolean extract(Slice row)
        {
            return type.getBoolean(readBlock(row.getInput()), index);
        }
    }

    private class LongFieldExtractor extends FieldExtractor<Long>
    {
        public LongFieldExtractor(Type type, int index)
        {
            super(type, index);
        }

        @Override
        public Long extract(Slice row)
        {
            return type.getLong(readBlock(row.getInput()), index);
        }
    }

    private class DoubleFieldExtractor extends FieldExtractor<Double>
    {
        public DoubleFieldExtractor(Type type, int index)
        {
            super(type, index);
        }

        @Override
        public Double extract(Slice row)
        {
            return type.getDouble(readBlock(row.getInput()), index);
        }
    }

    private class SliceFieldExtractor extends FieldExtractor<Slice>
    {
        public SliceFieldExtractor(Type type, int index)
        {
            super(type, index);
        }

        @Override
        public Slice extract(Slice row)
        {
            return type.getSlice(readBlock(row.getInput()), index);
        }
    }
}
