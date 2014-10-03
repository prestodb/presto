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
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;

public class MapToJsonCast
        extends ParametricOperator
{
    public static final MapToJsonCast MAP_TO_JSON = new MapToJsonCast();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
    private static final MethodHandle METHOD_HANDLE;

    static
    {
        try {
            METHOD_HANDLE = lookup().unreflect(MapToJsonCast.class.getMethod("toJson", Type.class, ConnectorSession.class, Slice.class));
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private MapToJsonCast()
    {
        super(OperatorType.CAST, ImmutableList.of(typeParameter("K"), typeParameter("V")), StandardTypes.JSON, ImmutableList.of("map<K,V>"));
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Type mapType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(keyType.getName(), valueType.getName()));
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(mapType);
        return operatorInfo(OperatorType.CAST, StandardTypes.JSON, ImmutableList.of(mapType.getName()), methodHandle, false, ImmutableList.of(false));
    }

    public static Slice toJson(Type arrayType, ConnectorSession session, Slice array)
    {
        Object object = stackRepresentationToObject(session, array, arrayType);
        try {
            return Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(object));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
