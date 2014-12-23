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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.google.common.base.Preconditions.checkArgument;

public class MapKeys
        extends ParametricScalar
{
    public static final MapKeys MAP_KEYS = new MapKeys();
    private static final Signature SIGNATURE = new Signature("map_keys", ImmutableList.of(typeParameter("K"), typeParameter("V")), "array<K>", ImmutableList.of("map<K,V>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapKeys.class, "getKeys", Type.class, Slice.class);
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Returns the keys of the given map<K,V> as an array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "map_keys expects only one argument");
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(keyType);
        Signature signature = new Signature("map_keys",
                parameterizedTypeName("array", keyType.getTypeSignature()),
                parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), true, ImmutableList.of(false));
    }

    public static Slice getKeys(Type keyType, Slice map)
    {
        List<Object> keys = new ArrayList<>();
        try (JsonParser parser = JSON_FACTORY.createJsonParser(map.getInput())) {
            JsonToken token = parser.nextToken();
            while (token != null) {
                if (token == JsonToken.FIELD_NAME) {
                    String fieldName = parser.getCurrentName();
                    keys.add(parseMapKeyAsType(fieldName, keyType));
                }
                token = parser.nextToken();
                parser.skipChildren();
            }
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return toStackRepresentation(keys);
    }

    private static Object parseMapKeyAsType(String value, Type t)
    {
        if (t.equals(VARCHAR) || t.equals(VARBINARY)) {
            return value;
        }

        if (t.equals(BIGINT) || t.equals(TIMESTAMP) || t.equals(DATE)) {
            return Long.valueOf(value);
        }

        if (t.equals(DOUBLE)) {
            return Double.valueOf(value);
        }

        if (t.equals(BOOLEAN)) {
            return Boolean.valueOf(value);
        }

        throw new PrestoException(NOT_SUPPORTED, "Type " + t + " not supported as a map key");
    }
}
