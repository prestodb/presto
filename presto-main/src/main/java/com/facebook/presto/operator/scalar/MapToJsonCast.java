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
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.MapType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.createBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class MapToJsonCast
        extends ParametricOperator
{
    public static final MapToJsonCast MAP_TO_JSON = new MapToJsonCast();
    private static final Supplier<ObjectMapper> OBJECT_MAPPER = Suppliers.memoize(() -> new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()).addSerializer(Map.class, new MapSerializer())));
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapToJsonCast.class, "toJson", Type.class, Type.class, ConnectorSession.class, Slice.class);

    private MapToJsonCast()
    {
        super(OperatorType.CAST, ImmutableList.of(typeParameter("K"), typeParameter("V")), StandardTypes.JSON, ImmutableList.of("map<K,V>"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Type mapType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(keyType);
        methodHandle = methodHandle.bindTo(valueType);

        return operatorInfo(OperatorType.CAST, parseTypeSignature(StandardTypes.JSON), ImmutableList.of(mapType.getTypeSignature()), methodHandle, false, ImmutableList.of(false));
    }

    public static Slice toJson(Type keyType, Type valueType, ConnectorSession session, Slice slice)
    {
        MapType mapType = new MapType(keyType, valueType);
        Object object = mapType.getObjectValue(session, createBlock(mapType, slice), 0);
        try {
            return Slices.utf8Slice(OBJECT_MAPPER.get().writeValueAsString(object));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    // Unfortunately this has to be a raw Map, since Map<?, ?> doesn't seem to work in Jackson
    private static class MapSerializer
            extends JsonSerializer<Map>
    {
        @Override
        public void serialize(Map map, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            Map<String, Object> orderedMap = new TreeMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
                orderedMap.put(entry.getKey().toString(), entry.getValue());
            }
            jsonGenerator.writeStartObject();
            for (Map.Entry<String, Object> entry : orderedMap.entrySet()) {
                jsonGenerator.writeObjectField(entry.getKey(), entry.getValue());
            }
            jsonGenerator.writeEndObject();
        }
    }
}
