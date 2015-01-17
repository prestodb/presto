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
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;
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
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class ArraySubscriptOperator
        extends ParametricOperator
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
    private static final CollectionType COLLECTION_TYPE = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, Object.class);

    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
            .put(boolean.class, methodHandle(ArraySubscriptOperator.class, "booleanSubscript", Slice.class, long.class))
            .put(long.class, methodHandle(ArraySubscriptOperator.class, "longSubscript", Slice.class, long.class))
            .put(void.class, methodHandle(ArraySubscriptOperator.class, "arrayWithUnknownType", Slice.class, long.class))
            .put(double.class, methodHandle(ArraySubscriptOperator.class, "doubleSubscript", Slice.class, long.class))
            .put(Slice.class, methodHandle(ArraySubscriptOperator.class, "sliceSubscript", Slice.class, long.class))
            .build();
    private static final MethodHandle STRUCTURAL_METHOD_HANDLE = methodHandle(ArraySubscriptOperator.class, "structuralSubscript", Slice.class, long.class);

    private static final LoadingCache<CacheKey, JsonExtractor<?>> CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .maximumSize(1000)
            .build(new CacheLoader<CacheKey, JsonExtractor<?>>()
            {
                @Override
                public JsonExtractor<?> load(CacheKey key)
                        throws Exception
                {
                    return generateExtractor(format("$[%d]", key.getIndex() - 1), key.getType().getExtractor(), true);
                }
            });

    protected ArraySubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>", "bigint"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected one type, got %s", types);
        Type elementType = types.get("E");

        MethodHandle methodHandle;
        if (elementType instanceof ArrayType || elementType instanceof MapType || elementType instanceof RowType) {
            methodHandle = STRUCTURAL_METHOD_HANDLE;
        }
        else {
            methodHandle = METHOD_HANDLES.get(elementType.getJavaType());
        }
        checkNotNull(methodHandle, "methodHandle is null");
        return new FunctionInfo(Signature.internalOperator(SUBSCRIPT.name(), elementType.getTypeSignature(), parameterizedTypeName("array", elementType.getTypeSignature()), parseTypeSignature(StandardTypes.BIGINT)), "Array subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    public static void arrayWithUnknownType(Slice json, long index)
    {
        try {
            List<Object> array = OBJECT_MAPPER.readValue(json.getInput(), COLLECTION_TYPE);
            if (index <= 0 || index > array.size()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Index out of bounds");
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
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

    @SuppressWarnings("unchecked")
    private static <T> T subscript(Slice array, long index, ExtractorType type)
    {
        if (index <= 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Index out of bounds");
        }
        JsonExtractor<?> extractor = CACHE.getUnchecked(new CacheKey(index, type));
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
