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
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedJacksonModule<T>
        extends SimpleModule
{
    private static final String TYPE_PROPERTY = "@type";
    private static final String DATA_PROPERTY = "customSerializedValue";

    protected AbstractTypedJacksonModule(
            Class<T> baseClass,
            Function<T, String> nameResolver,
            Function<String, Class<? extends T>> classResolver,
            boolean binarySerializationEnabled,
            Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor)
    {
        super(baseClass.getSimpleName() + "Module", Version.unknownVersion());

        requireNonNull(baseClass, "baseClass is null");
        requireNonNull(nameResolver, "nameResolver is null");
        requireNonNull(classResolver, "classResolver is null");
        requireNonNull(codecExtractor, "codecExtractor is null");

        if (binarySerializationEnabled) {
            // Use codec serialization
            addSerializer(baseClass, new CodecSerializer<>(nameResolver, classResolver, codecExtractor));
            addDeserializer(baseClass, new CodecDeserializer<>(classResolver, codecExtractor));
        }
        else {
            // Use legacy typed serialization
            TypeIdResolver typeResolver = new InternalTypeResolver<>(nameResolver, classResolver);
            addSerializer(baseClass, new InternalTypeSerializer<>(baseClass, typeResolver));
            addDeserializer(baseClass, new InternalTypeDeserializer<>(baseClass, typeResolver));
        }
    }

    private static class CodecSerializer<T>
            extends JsonSerializer<T>
    {
        private final Function<T, String> nameResolver;
        private final Function<String, Class<? extends T>> classResolver;
        private final Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor;
        private final TypeIdResolver typeResolver;
        private final TypeSerializer typeSerializer;
        private final Cache<Class<?>, JsonSerializer<Object>> serializerCache = CacheBuilder.newBuilder().build();

        public CodecSerializer(
                Function<T, String> nameResolver,
                Function<String, Class<? extends T>> classResolver,
                Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor)
        {
            this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
            this.classResolver = requireNonNull(classResolver, "classResolver is null");
            this.codecExtractor = requireNonNull(codecExtractor, "codecExtractor is null");
            this.typeResolver = new InternalTypeResolver<>(nameResolver, classResolver);
            this.typeSerializer = new AsPropertyTypeSerializer(typeResolver, null, TYPE_PROPERTY);
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
                    jsonGenerator.writeStringField(TYPE_PROPERTY, connectorIdString);
                    byte[] data = codec.get().serialize(value);
                    jsonGenerator.writeStringField(DATA_PROPERTY, Base64.getEncoder().encodeToString(data));
                    jsonGenerator.writeEndObject();
                    return;
                }
            }

            // Fall back to legacy typed JSON serialization
            // Use the InternalTypeSerializer approach which adds @type for polymorphic deserialization
            try {
                Class<?> type = value.getClass();
                JsonSerializer<Object> serializer = serializerCache.get(type, () -> createSerializer(provider, type));

                // Serialize with type information
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
            return (JsonSerializer<Object>) BeanSerializerFactory.instance.createSerializer(provider, javaType);
        }

        @Override
        public void serializeWithType(T value, JsonGenerator gen,
                                     SerializerProvider serializers, TypeSerializer typeSer)
                throws IOException
        {
            serialize(value, gen, serializers);
        }
    }

    private static class CodecDeserializer<T>
            extends JsonDeserializer<T>
    {
        private final Function<String, Class<? extends T>> classResolver;
        private final Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor;

        public CodecDeserializer(
                Function<String, Class<? extends T>> classResolver,
                Function<ConnectorId, Optional<ConnectorCodec<T>>> codecExtractor)
        {
            this.classResolver = requireNonNull(classResolver, "classResolver is null");
            this.codecExtractor = requireNonNull(codecExtractor, "codecExtractor is null");
        }

        @Override
        public T deserialize(JsonParser parser, DeserializationContext context)
                throws IOException
        {
            if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }

            if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
                throw new IOException("Expected START_OBJECT, got " + parser.getCurrentToken());
            }

            // Parse the JSON tree
            TreeNode tree = parser.readValueAsTree();

            if (tree instanceof ObjectNode) {
                ObjectNode node = (ObjectNode) tree;

                // Get the @type field
                if (!node.has(TYPE_PROPERTY)) {
                    throw new IOException("Missing " + TYPE_PROPERTY + " field");
                }
                String connectorIdString = node.get(TYPE_PROPERTY).asText();
                // Check if @data field is present (binary serialization)
                if (node.has(DATA_PROPERTY)) {
                    // Binary data is present, we need a codec to deserialize it
                    // Special handling for internal handles like "$remote"
                    if (!connectorIdString.startsWith("$")) {
                        ConnectorId connectorId = new ConnectorId(connectorIdString);
                        Optional<ConnectorCodec<T>> codec = codecExtractor.apply(connectorId);
                        if (codec.isPresent()) {
                            String base64Data = node.get(DATA_PROPERTY).asText();
                            byte[] data = Base64.getDecoder().decode(base64Data);
                            return codec.get().deserialize(data);
                        }
                    }
                    // @data field present but no codec available or internal handle
                    throw new IOException("Type " + connectorIdString + " has binary data (customSerializedValue field) but no codec available to deserialize it");
                }

                // No @data field - use standard JSON deserialization
                Class<? extends T> handleClass = classResolver.apply(connectorIdString);

                // Remove the @type field and deserialize the remaining content
                node.remove(TYPE_PROPERTY);
                return context.readTreeAsValue(node, handleClass);
            }

            throw new IOException("Unable to deserialize");
        }

        @Override
        public T deserializeWithType(JsonParser p, DeserializationContext ctxt,
                                     TypeDeserializer typeDeserializer)
                throws IOException
        {
            // We handle the type ourselves
            return deserialize(p, ctxt);
        }

        @Override
        public T deserializeWithType(JsonParser p, DeserializationContext ctxt,
                                     TypeDeserializer typeDeserializer, T intoValue)
                throws IOException
        {
            // We handle the type ourselves
            return deserialize(p, ctxt);
        }
    }

    // Legacy classes for backward compatibility
    private static class InternalTypeDeserializer<T>
            extends StdDeserializer<T>
    {
        private final TypeDeserializer typeDeserializer;

        public InternalTypeDeserializer(Class<T> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeDeserializer = new AsPropertyTypeDeserializer(
                    TypeFactory.defaultInstance().constructType(baseClass),
                    typeIdResolver,
                    TYPE_PROPERTY,
                    false,
                    null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return (T) typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext);
        }
    }

    private static class InternalTypeSerializer<T>
            extends StdSerializer<T>
    {
        private final TypeSerializer typeSerializer;
        private final Cache<Class<?>, JsonSerializer<T>> serializerCache = CacheBuilder.newBuilder().build();

        public InternalTypeSerializer(Class<T> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, TYPE_PROPERTY);
        }

        @Override
        public void serialize(T value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            if (value == null) {
                provider.defaultSerializeNull(generator);
                return;
            }

            try {
                Class<?> type = value.getClass();
                JsonSerializer<T> serializer = serializerCache.get(type, () -> createSerializer(provider, type));
                serializer.serializeWithType(value, generator, provider, typeSerializer);
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
        private static <T> JsonSerializer<T> createSerializer(SerializerProvider provider, Class<?> type)
                throws JsonMappingException
        {
            JavaType javaType = provider.constructType(type);
            return (JsonSerializer<T>) BeanSerializerFactory.instance.createSerializer(provider, javaType);
        }
    }

    private static class InternalTypeResolver<T>
            extends TypeIdResolverBase
    {
        private final Function<T, String> nameResolver;
        private final Function<String, Class<? extends T>> classResolver;

        public InternalTypeResolver(Function<T, String> nameResolver, Function<String, Class<? extends T>> classResolver)
        {
            this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
            this.classResolver = requireNonNull(classResolver, "classResolver is null");
        }

        @Override
        public String idFromValue(Object value)
        {
            return idFromValueAndType(value, value.getClass());
        }

        @SuppressWarnings("unchecked")
        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType)
        {
            requireNonNull(value, "value is null");
            String type = nameResolver.apply((T) value);
            checkArgument(type != null, "Unknown class: %s", suggestedType.getSimpleName());
            return type;
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id)
        {
            requireNonNull(id, "id is null");
            Class<?> typeClass = classResolver.apply(id);
            checkArgument(typeClass != null, "Unknown type ID: %s", id);
            return context.getTypeFactory().constructType(typeClass);
        }

        @Override
        public Id getMechanism()
        {
            return Id.NAME;
        }
    }
}
