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
package com.facebook.presto.server;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.base.Throwables.throwIfInstanceOf;

class CachingJacksonSerializer<U, V>
        extends StdSerializer<U>
{
    private final Cache<V, SerializableString> cache = CacheBuilder.newBuilder().weakValues().build();
    private final Function<U, V> keyingFunction;
    private final JsonSerializer<Object> innerSerializer;
    private final JsonFactory jsonFactory;

    public CachingJacksonSerializer(Class<U> beanClass, Function<U, V> keyingFunction, JsonSerializer<Object> innerSerializer, JsonFactory jsonFactory)
    {
        super(beanClass);
        this.keyingFunction = keyingFunction;
        this.innerSerializer = innerSerializer;
        this.jsonFactory = jsonFactory;
    }

    @Override
    public void serialize(U value, JsonGenerator generator, SerializerProvider provider)
            throws IOException
    {
        try {
            SerializableString serialized = cache.get(keyingFunction.apply(value), () -> serialize(value, provider, innerSerializer));
            generator.writeRawValue(serialized);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                throwIfInstanceOf(cause, IOException.class);
            }
            throw new RuntimeException(e);
        }
    }

    private SerializableString serialize(U value, SerializerProvider provider, JsonSerializer<Object> serializer)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (JsonGenerator innerGenerator = jsonFactory.createGenerator(outputStream)) {
            serializer.serialize(value, innerGenerator, provider);
        }
        return new SerializedString(new String(outputStream.toByteArray()));
    }
}
