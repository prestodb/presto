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

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedJacksonModule<T>
        extends SimpleModule
{
    private static final String TYPE_PROPERTY = "@type";

    protected AbstractTypedJacksonModule(Class<T> baseClass, JsonTypeIdResolver<T> typeIdResolver)
    {
        super(baseClass.getSimpleName() + "Module", Version.unknownVersion());

        TypeIdResolver typeResolver = new InternalTypeResolver((JsonTypeIdResolver<Object>) typeIdResolver);

        addSerializer(baseClass, new InternalTypeSerializer(baseClass, typeResolver));
        addDeserializer(baseClass, new InternalTypeDeserializer(baseClass, typeResolver));
    }

    public class InternalTypeDeserializer
            extends StdDeserializer<T>
    {
        private final TypeDeserializer typeDeserializer;

        InternalTypeDeserializer(Class<T> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeDeserializer = new AsPropertyTypeDeserializer(SimpleType.construct(baseClass), typeIdResolver, TYPE_PROPERTY, false, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return (T) typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext);
        }
    }

    public class InternalTypeSerializer
            extends StdSerializer<T>
    {
        private final TypeSerializer typeSerializer;
        private final Cache<Class<?>, JsonSerializer<Object>> serializerCache = CacheBuilder.newBuilder().build();

        InternalTypeSerializer(Class<T> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, TYPE_PROPERTY);
        }

        @Override
        public void serialize(final T value, JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException
        {
            if (value == null) {
                serializerProvider.defaultSerializeNull(jsonGenerator);
            }
            else {
                try {
                    JsonSerializer<Object> serializer = serializerCache.get(value.getClass(), new Callable<JsonSerializer<Object>>()
                    {
                        @Override
                        public JsonSerializer<Object> call()
                                throws Exception
                        {
                            return BeanSerializerFactory.instance.createSerializer(serializerProvider, serializerProvider.constructType(value.getClass()));
                        }
                    });

                    serializer.serializeWithType(value, jsonGenerator, serializerProvider, typeSerializer);
                }
                catch (ExecutionException e) {
                    Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
                    Throwables.propagateIfInstanceOf(e.getCause(), JsonGenerationException.class);
                    throw Throwables.propagate(e.getCause());
                }
            }
        }
    }

    class InternalTypeResolver
            implements TypeIdResolver
    {
        private final JsonTypeIdResolver<Object> typeIdResolver;
        private final LoadingCache<Class<?>, SimpleType> simpleTypes;

        InternalTypeResolver(JsonTypeIdResolver<Object> typeIdResolver)
        {
            this.typeIdResolver = requireNonNull(typeIdResolver, "typeIdResolver is null");
            simpleTypes = CacheBuilder.newBuilder().weakKeys().weakValues().build(new CacheLoader<Class<?>, SimpleType>()
            {
                @Override
                public SimpleType load(Class<?> typeClass)
                        throws Exception
                {
                    return SimpleType.construct(typeClass);
                }
            });
        }

        @Override
        public void init(JavaType baseType)
        {
        }

        @Override
        public String idFromValue(Object value)
        {
            requireNonNull(value, "value is null");
            return idFromValueAndType(value, value.getClass());
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType)
        {
            requireNonNull(value, "value is null");
            String type = typeIdResolver.getId(value);
            checkArgument(type != null, "Unknown class %s", suggestedType.getSimpleName());
            return type;
        }

        @Override
        public String idFromBaseType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public JavaType typeFromId(String id)
        {
            requireNonNull(id, "id is null");
            Class<?> typeClass = typeIdResolver.getType(id);
            checkArgument(typeClass != null, "Unknown type id %s", id);
            return simpleTypes.getUnchecked(typeClass);
        }

        @Override
        public Id getMechanism()
        {
            return Id.NAME;
        }
    }
}
