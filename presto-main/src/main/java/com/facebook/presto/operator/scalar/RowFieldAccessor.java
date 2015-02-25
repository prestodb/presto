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
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionRegistry.mangleFieldAccessor;
import static com.facebook.presto.operator.scalar.JsonExtract.BooleanJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.DoubleJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.LongJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ScalarValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static com.facebook.presto.type.RowType.RowField;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class RowFieldAccessor
        extends ParametricScalar
{
    private static final Map<String, MethodHandle> METHOD_HANDLE_MAP;

    private final Signature signature;
    private final MethodHandle methodHandle;

    static {
        ImmutableMap.Builder<String, MethodHandle> builder = ImmutableMap.builder();
        builder.put("long", Reflection.methodHandle(RowFieldAccessor.class, "longAccessor", JsonExtractor.class, Slice.class));
        builder.put("double", Reflection.methodHandle(RowFieldAccessor.class, "doubleAccessor", JsonExtractor.class, Slice.class));
        builder.put("boolean", Reflection.methodHandle(RowFieldAccessor.class, "booleanAccessor", JsonExtractor.class, Slice.class));
        builder.put("slice", Reflection.methodHandle(RowFieldAccessor.class, "sliceAccessor", JsonExtractor.class, Slice.class));
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

        JsonExtractor<?> extractor;
        if (returnType instanceof ArrayType || returnType instanceof MapType || returnType instanceof RowType) {
            extractor = new JsonValueJsonExtractor();
        }
        else if (returnType.getJavaType() == boolean.class) {
            extractor = new BooleanJsonExtractor();
        }
        else if (returnType.getJavaType() == long.class) {
            extractor = new LongJsonExtractor();
        }
        else if (returnType.getJavaType() == double.class) {
            extractor = new DoubleJsonExtractor();
        }
        else if (returnType.getJavaType() == Slice.class) {
            extractor = new ScalarValueJsonExtractor();
        }
        else {
            throw new IllegalArgumentException("Unsupported stack type: " + returnType.getJavaType());
        }
        extractor = generateExtractor(format("$[%d]", index), extractor, true);
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

    public static Long longAccessor(JsonExtractor<Long> extractor, Slice row)
    {
        return JsonExtract.extract(row, extractor);
    }

    public static Boolean booleanAccessor(JsonExtractor<Boolean> extractor, Slice row)
    {
        return JsonExtract.extract(row, extractor);
    }

    public static Double doubleAccessor(JsonExtractor<Double> extractor, Slice row)
    {
        return JsonExtract.extract(row, extractor);
    }

    public static Slice sliceAccessor(JsonExtractor<Slice> extractor, Slice row)
    {
        return JsonExtract.extract(row, extractor);
    }
}
