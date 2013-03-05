package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractTypedJacksonModule<T>
        extends SimpleModule
{
    private final String typeProperty;

    protected AbstractTypedJacksonModule(Class<T> baseClass, String typeProperty, Map<String, Class<? extends T>> types)
    {
        super(baseClass.getSimpleName() + "Module", Version.unknownVersion());
        this.typeProperty = typeProperty;

        TypeIdResolver typeResolver = new InternalTypeResolver(types);

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
            this.typeDeserializer = new AsPropertyTypeDeserializer(SimpleType.construct(baseClass), typeIdResolver, typeProperty, false, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException
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
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, typeProperty);
        }

        @Override
        public void serialize(final T value, JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException
        {
            if (value == null) {
                serializerProvider.defaultSerializeNull(jsonGenerator);
            }
            else {
                try {
                    JsonSerializer<Object> serializer = serializerCache.get(value.getClass(), new Callable<JsonSerializer<Object>>() {

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
        private final BiMap<String, Class<? extends T>> types;
        private final Map<Class<? extends T>, SimpleType> simpleTypes;

        InternalTypeResolver(Map<String, Class<? extends T>> types)
        {
            this.types = ImmutableBiMap.copyOf(types);

            ImmutableMap.Builder<Class<? extends T>, SimpleType> builder = ImmutableMap.builder();
            for (Class<? extends T> handleClass : this.types.values()) {
                builder.put(handleClass, SimpleType.construct(handleClass));
            }
            this.simpleTypes = builder.build();
        }

        @Override
        public void init(JavaType baseType)
        {
        }

        @Override
        public String idFromValue(Object value)
        {
            checkNotNull(value, "value was null!");
            return idFromValueAndType(value, value.getClass());
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType)
        {
            String type = types.inverse().get(suggestedType);
            checkState(type != null, "Class %s is unknown!", suggestedType.getSimpleName());
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
            Class<?> typeClass = types.get(id);
            checkState(typeClass != null, "Type %s is unknown!", id);
            return simpleTypes.get(typeClass);
        }

        @Override
        public Id getMechanism()
        {
            return Id.NAME;
        }
    }
}
