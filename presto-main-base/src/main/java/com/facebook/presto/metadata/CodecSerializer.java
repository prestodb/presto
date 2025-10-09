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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorId;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

class CodecSerializer<T>
        extends JsonSerializer<T>
{
    private final Function<T, String> nameResolver;
    private final Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor;
    private final TypeIdResolver typeResolver;
    private final TypeSerializer typeSerializer;
    private final Cache<Class<?>, JsonSerializer<Object>> serializerCache = CacheBuilder.newBuilder().build();
    private final String typePropertyName;
    private final String dataPropertyName;

    public CodecSerializer(
            String typePropertyName,
            String dataPropertyName,
            Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor,
            Function<T, String> nameResolver,
            TypeIdResolver typeIdResolver)
    {
        this.typePropertyName = requireNonNull(typePropertyName, "typePropertyName is null");
        this.dataPropertyName = requireNonNull(dataPropertyName, "dataPropertyName is null");
        this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
        this.codecExtractor = requireNonNull(codecExtractor, "codecExtractor is null");
        this.typeResolver = requireNonNull(typeIdResolver, "typeIdResolver is null");
        this.typeSerializer = new AsPropertyTypeSerializer(typeResolver, null, typePropertyName);
    }

    @Override
    public void serialize(T value, JsonGenerator jsonGenerator, SerializerProvider provider)
            throws IOException
    {
        if (value == null) {
            jsonGenerator.writeNull();
            return;
        }

        String connectorIdString = nameResolver.apply(value);

        // Only try binary serialization for actual connectors (not internal handles like "$remote")
        if (!connectorIdString.startsWith("$")) {
            ConnectorId connectorId = new ConnectorId(connectorIdString);

            // Check if connector has a binary codec
            Optional<ConnectorCodec<T>> codec = codecExtractor.apply(connectorId);
            if (codec.isPresent()) {
                // Use binary serialization with flat structure
                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField(typePropertyName, connectorIdString);
                byte[] data = codec.get().serialize(value);
                jsonGenerator.writeStringField(dataPropertyName, Base64.getEncoder().encodeToString(data));
                jsonGenerator.writeEndObject();
                return;
            }
        }

        // Fall back to legacy typed JSON serialization
        try {
            Class<?> type = value.getClass();
            JsonSerializer<Object> serializer = serializerCache.get(type, () -> createSerializer(provider, type));
            serializer.serializeWithType(value, jsonGenerator, provider, typeSerializer);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                throwIfInstanceOf(cause, IOException.class);
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static JsonSerializer<Object> createSerializer(SerializerProvider provider, Class<?> type)
            throws JsonMappingException
    {
        JavaType javaType = provider.constructType(type);
        return BeanSerializerFactory.instance.createSerializer(provider, javaType);
    }

    @Override
    public void serializeWithType(T value, JsonGenerator gen,
            SerializerProvider serializers, TypeSerializer typeSer)
            throws IOException
    {
        serialize(value, gen, serializers);
    }
}
