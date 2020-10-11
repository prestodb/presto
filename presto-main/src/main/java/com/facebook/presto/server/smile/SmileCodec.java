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
package com.facebook.presto.server.smile;

import com.facebook.presto.server.codec.Codec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.Beta;
import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Beta
public class SmileCodec<T>
        implements Codec<T>
{
    private static final Supplier<ObjectMapper> OBJECT_MAPPER_SUPPLIER =
            Suppliers.memoize(() -> new SmileObjectMapperProvider().get());

    public static <T> SmileCodec<T> smileCodec(Class<T> type)
    {
        requireNonNull(type, "type is null");

        return new SmileCodec<>(OBJECT_MAPPER_SUPPLIER.get(), type);
    }

    public static <T> SmileCodec<T> smileCodec(TypeToken<T> type)
    {
        requireNonNull(type, "type is null");

        return new SmileCodec<>(OBJECT_MAPPER_SUPPLIER.get(), type.getType());
    }

    public static <T> SmileCodec<List<T>> listSmileCodec(Class<T> type)
    {
        requireNonNull(type, "type is null");

        Type listType = new TypeToken<List<T>>() {}
                .where(new TypeParameter<T>() {}, type)
                .getType();

        return new SmileCodec<>(OBJECT_MAPPER_SUPPLIER.get(), listType);
    }

    public static <T> SmileCodec<List<T>> listSmileCodec(SmileCodec<T> type)
    {
        requireNonNull(type, "type is null");

        Type listType = new TypeToken<List<T>>() {}
                .where(new TypeParameter<T>() {}, type.getTypeToken())
                .getType();

        return new SmileCodec<>(OBJECT_MAPPER_SUPPLIER.get(), listType);
    }

    public static <K, V> SmileCodec<Map<K, V>> mapSmileCodec(Class<K> keyType, Class<V> valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        Type mapType = new TypeToken<Map<K, V>>() {}
                .where(new TypeParameter<K>() {}, keyType)
                .where(new TypeParameter<V>() {}, valueType)
                .getType();

        return new SmileCodec<>(OBJECT_MAPPER_SUPPLIER.get(), mapType);
    }

    public static <K, V> SmileCodec<Map<K, V>> mapSmileCodec(Class<K> keyType, SmileCodec<V> valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        Type mapType = new TypeToken<Map<K, V>>() {}
                .where(new TypeParameter<K>() {}, keyType)
                .where(new TypeParameter<V>() {}, valueType.getTypeToken())
                .getType();

        return new SmileCodec<>(OBJECT_MAPPER_SUPPLIER.get(), mapType);
    }

    private final ObjectMapper mapper;
    private final Type type;
    private final JavaType javaType;

    SmileCodec(ObjectMapper mapper, Type type)
    {
        this.mapper = mapper;
        this.type = type;
        this.javaType = mapper.getTypeFactory().constructType(type);
    }

    /**
     * Gets the type this codec supports.
     */
    public Type getType()
    {
        return type;
    }

    /**
     * Converts the specified smile bytes (UTF-8) into an instance of type T.
     *
     * @param bytes the bytes (UTF-8) to parse
     * @return parsed response; never null
     * @throws IllegalArgumentException if the bytes bytes can not be converted to the type T
     */
    public T fromSmile(byte[] bytes)
            throws IllegalArgumentException
    {
        try {
            return mapper.readValue(bytes, javaType);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Invalid SMILE bytes for %s", javaType), e);
        }
    }

    /**
     * Converts the specified instance to smile encoded bytes.
     *
     * @param instance the instance to convert to smile encoded bytes
     * @return smile encoded bytes (UTF-8)
     * @throws IllegalArgumentException if the specified instance can not be converted to smile
     */
    @Override
    public byte[] toBytes(T instance)
            throws IllegalArgumentException
    {
        try {
            return mapper.writeValueAsBytes(instance);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("%s could not be converted to SMILE", instance.getClass().getName()), e);
        }
    }

    @Override
    public T fromBytes(byte[] bytes)
    {
        return fromSmile(bytes);
    }

    @SuppressWarnings("unchecked")
    TypeToken<T> getTypeToken()
    {
        return (TypeToken<T>) TypeToken.of(type);
    }
}
