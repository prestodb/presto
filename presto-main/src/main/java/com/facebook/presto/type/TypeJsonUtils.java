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
package com.facebook.presto.type;

import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Types.checkType;
import static java.lang.String.format;

public final class TypeJsonUtils
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
    private static final Set<Type> PASSTHROUGH_TYPES = ImmutableSet.<Type>of(BIGINT, DOUBLE, BOOLEAN, VARCHAR);

    private TypeJsonUtils() {}

    public static Object stackRepresentationToObject(ConnectorSession session, Object value, Type type)
    {
        if (value == null) {
            return null;
        }

        if (type instanceof ArrayType) {
            return arrayStackRepresentationToObject(session, checkType(value, String.class, "value"), ((ArrayType) type).getElementType());
        }

        if (type instanceof MapType) {
            return mapStackRepresentationToObject(session, checkType(value, String.class, "value"), ((MapType) type).getKeyType(), ((MapType) type).getValueType());
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, checkType(value, Boolean.class, "value"));
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, checkType(value, Long.class, "value"));
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, checkType(value, Double.class, "value"));
        }
        else if (type.getJavaType() == Slice.class) {
            if (value instanceof String) {
                value = Slices.utf8Slice((String) value);
            }
            type.writeSlice(blockBuilder, checkType(value, Slice.class, "value"));
        }
        return type.getObjectValue(session, blockBuilder.build(), 0);
    }

    private static List<Object> arrayStackRepresentationToObject(ConnectorSession session, String stackRepresentation, Type elementType)
    {
        Class<?> elementJsonType = stackTypeToJsonClass(elementType);

        JavaType listType = OBJECT_MAPPER.getTypeFactory().constructParametricType(List.class, elementJsonType);
        List<Object> stackElements;
        try {
            stackElements = OBJECT_MAPPER.readValue(stackRepresentation, listType);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        // Fast path for BIGINT, DOUBLE, BOOLEAN, and VARCHAR
        if (PASSTHROUGH_TYPES.contains(elementType)) {
            return stackElements;
        }

        // Convert stack types to objects
        List<Object> objectElements = new ArrayList<>();
        for (Object value : stackElements) {
            objectElements.add(stackRepresentationToObject(session, value, elementType));
        }

        return objectElements;
    }

    private static Map<Object, Object> mapStackRepresentationToObject(ConnectorSession session, String stackRepresentation, Type keyType, Type valueType)
    {
        Class<?> valueJsonType = stackTypeToJsonClass(valueType);

        JavaType listType = OBJECT_MAPPER.getTypeFactory().constructParametricType(Map.class, String.class, valueJsonType);
        Map<String, Object> stackMap;
        try {
            stackMap = OBJECT_MAPPER.readValue(stackRepresentation, listType);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        // Convert stack types to objects
        Map<Object, Object> objectMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : stackMap.entrySet()) {
            Object key = stackRepresentationToObject(session, castJsonMapKey(entry.getKey(), keyType), keyType);
            Object value = stackRepresentationToObject(session, entry.getValue(), valueType);
            objectMap.put(key, value);
        }

        return objectMap;
    }

    private static Object castJsonMapKey(String key, Type keyType)
    {
        if (keyType.getJavaType() == boolean.class) {
            return Boolean.valueOf(key);
        }
        else if (keyType.getJavaType() == long.class) {
            return Long.valueOf(key);
        }
        else if (keyType.getJavaType() == double.class) {
            return Double.valueOf(key);
        }
        else if (keyType.getJavaType() == Slice.class) {
            return key;
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported stack type: %s", keyType.getJavaType()));
        }
    }

    private static Class<?> stackTypeToJsonClass(Type type)
    {
        if (type.getJavaType() == boolean.class) {
            return Boolean.class;
        }
        else if (type.getJavaType() == long.class) {
            return Long.class;
        }
        else if (type.getJavaType() == double.class) {
            return Double.class;
        }
        else if (type.getJavaType() == Slice.class) {
            return String.class;
        }
        else if (type.getJavaType() == void.class) {
            return Void.class;
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported stack type: %s", type.getJavaType()));
        }
    }
}
