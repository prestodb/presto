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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.MapType;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.scalar.JsonExtract.JsonExtractor;
import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class MapSubscriptOperator
        extends ParametricOperator
{
    public static final MapSubscriptOperator MAP_SUBSCRIPT = new MapSubscriptOperator();
    private static final JsonExtractor<Long> LONG_JSON_EXTRACTOR = new LongJsonExtractor();
    private static final JsonExtractor<Boolean> BOOLEAN_JSON_EXTRACTOR = new BooleanJsonExtractor();
    private static final JsonExtractor<Double> DOUBLE_JSON_EXTRACTOR = new DoubleJsonExtractor();
    private static final JsonExtractor<Slice> SLICE_JSON_EXTRACTOR = new JsonExtract.ScalarValueJsonExtractor();

    protected MapSubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("K"), typeParameter("V")), "V", ImmutableList.of("map<K,V>", "K"));
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public FunctionInfo specialize(List<? extends Type> types)
    {
        checkArgument(types.size() == 2, "Expected two types, got %s", types);
        checkArgument(types.get(0) instanceof MapType, "Expected a map type as the first type");
        MapType mapType = (MapType) types.get(0);
        checkArgument(mapType.getKeyType().equals(types.get(1)), "Unsupported key type: %s, for %s", types.get(1), mapType.getName());
        Type valueType = mapType.getValueType();
        Type keyType = mapType.getKeyType();

        return new FunctionInfo(new Signature(SUBSCRIPT.name(), valueType.getName(), mapType.getName(), keyType.getName()), "Map subscript", true, lookupMethod(keyType.getJavaType(), valueType.getJavaType()), true, true, ImmutableList.of(false, false));
    }

    @Override
    public FunctionInfo specialize(Type returnType, List<? extends Type> types)
    {
        return specialize(types);
    }

    private static MethodHandle lookupMethod(Class<?> keyJavaType, Class<?> valueJavaType)
    {
        String methodName = keyJavaType.getSimpleName() + valueJavaType.getSimpleName() + "Subscript";
        try {
            return lookup().unreflect(MapSubscriptOperator.class.getMethod(methodName, Slice.class, keyJavaType));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Long SlicelongSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), LONG_JSON_EXTRACTOR);
    }

    public static Boolean SlicebooleanSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), BOOLEAN_JSON_EXTRACTOR);
    }

    public static Double SlicedoubleSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), DOUBLE_JSON_EXTRACTOR);
    }

    public static Slice SliceSliceSubscript(Slice map, Slice key)
    {
        return subscript(map, key.toStringUtf8(), SLICE_JSON_EXTRACTOR);
    }

    public static Long doublelongSubscript(Slice map, double key)
    {
        return subscript(map, key, LONG_JSON_EXTRACTOR);
    }

    public static Boolean doublebooleanSubscript(Slice map, double key)
    {
        return subscript(map, key, BOOLEAN_JSON_EXTRACTOR);
    }

    public static Double doubledoubleSubscript(Slice map, double key)
    {
        return subscript(map, key, DOUBLE_JSON_EXTRACTOR);
    }

    public static Slice doubleSliceSubscript(Slice map, double key)
    {
        return subscript(map, key, SLICE_JSON_EXTRACTOR);
    }

    public static Long booleanlongSubscript(Slice map, boolean key)
    {
        return subscript(map, key, LONG_JSON_EXTRACTOR);
    }

    public static Boolean booleanbooleanSubscript(Slice map, boolean key)
    {
        return subscript(map, key, BOOLEAN_JSON_EXTRACTOR);
    }

    public static Double booleandoubleSubscript(Slice map, boolean key)
    {
        return subscript(map, key, DOUBLE_JSON_EXTRACTOR);
    }

    public static Slice booleanSliceSubscript(Slice map, boolean key)
    {
        return subscript(map, key, SLICE_JSON_EXTRACTOR);
    }

    public static Long longlongSubscript(Slice map, long key)
    {
        return subscript(map, key, LONG_JSON_EXTRACTOR);
    }

    public static Boolean longbooleanSubscript(Slice map, long key)
    {
        return subscript(map, key, BOOLEAN_JSON_EXTRACTOR);
    }

    public static Double longdoubleSubscript(Slice map, long key)
    {
        return subscript(map, key, DOUBLE_JSON_EXTRACTOR);
    }

    public static Slice longSliceSubscript(Slice map, long key)
    {
        return subscript(map, key, SLICE_JSON_EXTRACTOR);
    }

    private static <T> T subscript(Slice map, Object key, JsonExtractor<T> extractor)
    {
        extractor = generateExtractor(format("$[\"%s\"]", key.toString()), extractor);
        return JsonExtract.extract(map, extractor);
    }

    public static class BooleanJsonExtractor
            implements JsonExtractor<Boolean>
    {
        @Override
        public Boolean extract(JsonParser jsonParser)
                throws IOException
        {
            JsonToken token = jsonParser.getCurrentToken();
            if (token == null) {
                throw new JsonParseException("Unexpected end of value", jsonParser.getCurrentLocation());
            }
            if (!token.isScalarValue() || token == VALUE_NULL) {
                return null;
            }
            return jsonParser.getBooleanValue();
        }
    }

    public static class DoubleJsonExtractor
            implements JsonExtractor<Double>
    {
        @Override
        public Double extract(JsonParser jsonParser)
                throws IOException
        {
            JsonToken token = jsonParser.getCurrentToken();
            if (token == null) {
                throw new JsonParseException("Unexpected end of value", jsonParser.getCurrentLocation());
            }
            if (!token.isScalarValue() || token == VALUE_NULL) {
                return null;
            }
            return jsonParser.getDoubleValue();
        }
    }

    public static class LongJsonExtractor
            implements JsonExtractor<Long>
    {
        @Override
        public Long extract(JsonParser jsonParser)
                throws IOException
        {
            JsonToken token = jsonParser.getCurrentToken();
            if (token == null) {
                throw new JsonParseException("Unexpected end of value", jsonParser.getCurrentLocation());
            }
            if (!token.isScalarValue() || token == VALUE_NULL) {
                return null;
            }
            return jsonParser.getLongValue();
        }
    }
}
