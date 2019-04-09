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
package com.facebook.presto.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.inject.Binder;

import javax.inject.Provider;

import java.io.IOException;

import static io.airlift.json.JsonBinder.jsonBinder;

public class MapJsonKeyUtils
{
    private MapJsonKeyUtils()
    {
    }

    public static <T> JsonSerializer<T> createKeySerializer(Provider<ObjectMapper> mapperProvider)
    {
        return new JsonSerializer<T>()
        {
            @Override
            public void serialize(T value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException
            {
                gen.writeFieldName(mapperProvider.get().writeValueAsString(value));
            }
        };
    }

    public static <T> KeyDeserializer createKeyDeserializer(Class<T> clazz, Provider<ObjectMapper> mapperProvider)
    {
        return new KeyDeserializer()
        {
            @Override
            public Object deserializeKey(String key, DeserializationContext ctxt)
                    throws IOException
            {
                return mapperProvider.get().readValue(key, clazz);
            }
        };
    }

    public static <T> void bindMapKeySerde(Binder binder, Class<T> clazz)
    {
        jsonBinder(binder).addKeySerializerBinding(clazz).toProvider(new Provider<JsonSerializer<?>>()
        {
            private Provider<ObjectMapper> mapperProvider = binder.getProvider(ObjectMapper.class);

            @Override
            public JsonSerializer<?> get()
            {
                return createKeySerializer(mapperProvider);
            }
        });
        jsonBinder(binder).addKeyDeserializerBinding(clazz).toProvider(new Provider<KeyDeserializer>()
        {
            private Provider<ObjectMapper> mapperProvider = binder.getProvider(ObjectMapper.class);

            @Override
            public KeyDeserializer get()
            {
                return createKeyDeserializer(clazz, mapperProvider);
            }
        });
    }
}
