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
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.scalar.JsonExtract.BooleanJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.DoubleJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.LongJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.ScalarValueJsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class ArraySubscriptOperator
        extends ParametricOperator
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();
    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES;
    private static final MethodHandle STRUCTURAL_METHOD_HANDLE;
    private static final LoadingCache<CacheKey, JsonExtractor<?>> cache;

    static {
        ImmutableMap.Builder<Class<?>, MethodHandle> builder = ImmutableMap.builder();
        try {
            builder.put(boolean.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("booleanSubscript", Slice.class, long.class)));
            builder.put(long.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("longSubscript", Slice.class, long.class)));
            builder.put(double.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("doubleSubscript", Slice.class, long.class)));
            builder.put(Slice.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("sliceSubscript", Slice.class, long.class)));
            STRUCTURAL_METHOD_HANDLE = lookup().unreflect(ArraySubscriptOperator.class.getMethod("structuralSubscript", Slice.class, long.class));
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        METHOD_HANDLES = builder.build();

        cache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES).maximumSize(1000).build(new CacheLoader<CacheKey, JsonExtractor<?>>() {
            @Override
            public JsonExtractor<?> load(CacheKey key)
                    throws Exception
            {
                return generateExtractor(format("$[%d]", key.getIndex() - 1), key.getType().getExtractor(), true);
            }
        });
    }

    protected ArraySubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>", "bigint"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");

        MethodHandle methodHandle;
        if (elementType instanceof ArrayType || elementType instanceof MapType) {
            methodHandle = STRUCTURAL_METHOD_HANDLE;
        }
        else {
            methodHandle = METHOD_HANDLES.get(elementType.getJavaType());
        }
        checkNotNull(methodHandle, "methodHandle is null");
        return new FunctionInfo(Signature.internalOperator(SUBSCRIPT.name(), elementType.getName(), parameterizedTypeName("array", elementType.getName()), StandardTypes.BIGINT), "Array subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    public static Long longSubscript(Slice array, long index)
    {
        return subscript(array, index, ExtractorType.LONG);
    }

    public static Boolean booleanSubscript(Slice array, long index)
    {
        return subscript(array, index, ExtractorType.BOOLEAN);
    }

    public static Double doubleSubscript(Slice array, long index)
    {
        return subscript(array, index, ExtractorType.DOUBLE);
    }

    public static Slice sliceSubscript(Slice array, long index)
    {
        return subscript(array, index, ExtractorType.SLICE);
    }

    public static Slice structuralSubscript(Slice array, long index)
    {
        return subscript(array, index, ExtractorType.STRUCTURAL);
    }

    private static <T> T subscript(Slice array, long index, ExtractorType type)
    {
        if (index == 0) {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT.toErrorCode(), "Index out of bounds");
        }
        JsonExtractor<?> extractor = cache.getUnchecked(new CacheKey(index, type));
        return (T) JsonExtract.extract(array, extractor);
    }

    private static class CacheKey
    {
        private final long index;
        private final ExtractorType type;

        private CacheKey(long index, ExtractorType type)
        {
            this.index = index;
            this.type = type;
        }

        public long getIndex()
        {
            return index;
        }

        public ExtractorType getType()
        {
            return type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CacheKey cacheKey = (CacheKey) o;

            if (index != cacheKey.index) {
                return false;
            }
            if (type != cacheKey.type) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (index ^ (index >>> 32));
            result = 31 * result + type.hashCode();
            return result;
        }
    }

    private enum ExtractorType
    {
        LONG(new LongJsonExtractor()),
        BOOLEAN(new BooleanJsonExtractor()),
        DOUBLE(new DoubleJsonExtractor()),
        SLICE(new ScalarValueJsonExtractor()),
        STRUCTURAL(new JsonValueJsonExtractor());

        private final JsonExtractor<?> extractor;

        ExtractorType(JsonExtractor<?> extractor)
        {
            this.extractor = extractor;
        }

        public JsonExtractor<?> getExtractor()
        {
            return extractor;
        }
    }
}
