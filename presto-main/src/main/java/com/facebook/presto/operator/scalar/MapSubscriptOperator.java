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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
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
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class MapSubscriptOperator
        extends ParametricOperator
{
    public static final MapSubscriptOperator MAP_SUBSCRIPT = new MapSubscriptOperator();

    private static final LoadingCache<CacheKey, JsonExtractor<?>> CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .maximumSize(1000)
            .build(new CacheLoader<CacheKey, JsonExtractor<?>>()
            {
                @Override
                public JsonExtractor<?> load(CacheKey key)
                        throws Exception
                {
                    return generateExtractor(format("$[\"%s\"]", key.getKey()), key.getType().getExtractor());
                }
            });

    protected MapSubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("K"), typeParameter("V")), "V", ImmutableList.of("map<K,V>", "K"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        Signature signature = new Signature(SUBSCRIPT.name(), valueType.getTypeSignature(), parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()), keyType.getTypeSignature());
        return new FunctionInfo(signature, "Map subscript", true, lookupMethod(keyType, valueType), true, true, ImmutableList.of(false, false));
    }

    private static MethodHandle lookupMethod(Type keyType, Type valueType)
    {
        String methodName = keyType.getJavaType().getSimpleName();
        if (valueType instanceof ArrayType || valueType instanceof MapType || valueType instanceof RowType) {
            methodName += "Structural";
        }
        else {
            methodName += valueType.getJavaType().getSimpleName();
        }
        methodName += "Subscript";
        try {
            return lookup().unreflect(MapSubscriptOperator.class.getMethod(methodName, Slice.class, keyType.getJavaType()));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Long SlicelongSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), ExtractorType.LONG);
    }

    public static Boolean SlicebooleanSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), ExtractorType.BOOLEAN);
    }

    public static Double SlicedoubleSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), ExtractorType.DOUBLE);
    }

    public static Slice SliceSliceSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), ExtractorType.SLICE);
    }

    public static Slice SliceStructuralSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), ExtractorType.STRUCTURAL);
    }

    public static Long doublelongSubscript(Slice map, double key)
    {
        return subscript(map, key, ExtractorType.LONG);
    }

    public static Boolean doublebooleanSubscript(Slice map, double key)
    {
        return subscript(map, key, ExtractorType.BOOLEAN);
    }

    public static Double doubledoubleSubscript(Slice map, double key)
    {
        return subscript(map, key, ExtractorType.DOUBLE);
    }

    public static Slice doubleSliceSubscript(Slice map, double key)
    {
        return subscript(map, key, ExtractorType.SLICE);
    }

    public static Slice doubleStructuralSubscript(Slice map, double key)
    {
        return subscript(map, key, ExtractorType.STRUCTURAL);
    }

    public static Long booleanlongSubscript(Slice map, boolean key)
    {
        return subscript(map, key, ExtractorType.LONG);
    }

    public static Boolean booleanbooleanSubscript(Slice map, boolean key)
    {
        return subscript(map, key, ExtractorType.BOOLEAN);
    }

    public static Double booleandoubleSubscript(Slice map, boolean key)
    {
        return subscript(map, key, ExtractorType.DOUBLE);
    }

    public static Slice booleanSliceSubscript(Slice map, boolean key)
    {
        return subscript(map, key, ExtractorType.SLICE);
    }

    public static Slice booleanStructuralSubscript(Slice map, boolean key)
    {
        return subscript(map, key, ExtractorType.STRUCTURAL);
    }

    public static Long longlongSubscript(Slice map, long key)
    {
        return subscript(map, key, ExtractorType.LONG);
    }

    public static Boolean longbooleanSubscript(Slice map, long key)
    {
        return subscript(map, key, ExtractorType.BOOLEAN);
    }

    public static Double longdoubleSubscript(Slice map, long key)
    {
        return subscript(map, key, ExtractorType.DOUBLE);
    }

    public static Slice longSliceSubscript(Slice map, long key)
    {
        return subscript(map, key, ExtractorType.SLICE);
    }

    public static Slice longStructuralSubscript(Slice map, long key)
    {
        return subscript(map, key, ExtractorType.STRUCTURAL);
    }

    @SuppressWarnings("unchecked")
    private static <T> T subscript(Slice map, Object key, ExtractorType type)
    {
        JsonExtractor<?> extractor = CACHE.getUnchecked(new CacheKey(key.toString(), type));
        return (T) JsonExtract.extract(map, extractor);
    }

    private static class CacheKey
    {
        private final String key;
        private final ExtractorType type;

        private CacheKey(String key, ExtractorType type)
        {
            this.key = key;
            this.type = type;
        }

        public String getKey()
        {
            return key;
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

            if (!key.equals(cacheKey.key)) {
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
            int result = key.hashCode();
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
