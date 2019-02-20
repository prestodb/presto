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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Beta
public class SmileCodecFactory
{
    private final Provider<ObjectMapper> objectMapperProvider;

    public SmileCodecFactory()
    {
        this(new SmileObjectMapperProvider());
    }

    @Inject
    public SmileCodecFactory(@ForSmile Provider<ObjectMapper> smileObjectMapperProvider)
    {
        this.objectMapperProvider = requireNonNull(smileObjectMapperProvider, "smileObjectMapperProvider is null");
    }

    public <T> SmileCodec<T> smileCodec(Class<T> type)
    {
        requireNonNull(type, "type is null");

        return new SmileCodec<>(createObjectMapper(), type);
    }

    public <T> SmileCodec<T> smileCodec(Type type)
    {
        requireNonNull(type, "type is null");

        return new SmileCodec<>(createObjectMapper(), type);
    }

    public <T> SmileCodec<T> smileCodec(TypeToken<T> type)
    {
        requireNonNull(type, "type is null");

        return new SmileCodec<>(createObjectMapper(), type.getType());
    }

    public <T> SmileCodec<List<T>> listSmileCodec(Class<T> type)
    {
        requireNonNull(type, "type is null");

        Type listType = new TypeToken<List<T>>() {}
                .where(new TypeParameter<T>() {}, type)
                .getType();

        return new SmileCodec<>(createObjectMapper(), listType);
    }

    public <T> SmileCodec<List<T>> listSmileCodec(SmileCodec<T> type)
    {
        requireNonNull(type, "type is null");

        Type listType = new TypeToken<List<T>>() {}
                .where(new TypeParameter<T>() {}, type.getTypeToken())
                .getType();

        return new SmileCodec<>(createObjectMapper(), listType);
    }

    public <K, V> SmileCodec<Map<K, V>> mapSmileCodec(Class<K> keyType, Class<V> valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        Type mapType = new TypeToken<Map<K, V>>() {}
                .where(new TypeParameter<K>() {}, keyType)
                .where(new TypeParameter<V>() {}, valueType)
                .getType();

        return new SmileCodec<>(createObjectMapper(), mapType);
    }

    public <K, V> SmileCodec<Map<K, V>> mapSmileCodec(Class<K> keyType, SmileCodec<V> valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        Type mapType = new TypeToken<Map<K, V>>() {}
                .where(new TypeParameter<K>() {}, keyType)
                .where(new TypeParameter<V>() {}, valueType.getTypeToken())
                .getType();

        return new SmileCodec<>(createObjectMapper(), mapType);
    }

    private ObjectMapper createObjectMapper()
    {
        return objectMapperProvider.get();
    }
}
