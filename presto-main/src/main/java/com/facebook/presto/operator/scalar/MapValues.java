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
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.MapType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class MapValues
        extends ParametricScalar
{
    public static final MapValues MAP_VALUES = new MapValues();
    private static final Signature SIGNATURE = new Signature("map_values", ImmutableList.of(typeParameter("K"), typeParameter("V")), "array<V>", ImmutableList.of("map<K,V>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapValues.class, "getValues", Slice.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));

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
        return "Returns the values of the given map<K,V> as an array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "map_values expects only one argument");
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Signature signature = new Signature("map_values",
                parameterizedTypeName("array", valueType.getTypeSignature()),
                parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()));
        return new FunctionInfo(signature, getDescription(), isHidden(), METHOD_HANDLE, isDeterministic(), true, ImmutableList.of(false));
    }

    public static Slice getValues(Slice map)
    {
        List<Object> values = new ArrayList<>(); // allow nulls
        MapType type = OBJECT_MAPPER.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
        Map<String, Object> jsonMap;
        try {
            jsonMap = OBJECT_MAPPER.readValue(map.getInput(), type);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        values.addAll(jsonMap.values());
        return toStackRepresentation(values);
    }
}
