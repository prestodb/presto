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
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.MapBinder;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableHandleJacksonModule
        extends SimpleModule
        implements TypeIdResolver
{
    public static LinkedBindingBuilder<Class<? extends TableHandle>> bindTableHandle(Binder binder, String tableHandlerType)
    {
        MapBinder<String, Class<? extends TableHandle>> tableHandleTypes = MapBinder.newMapBinder(
                binder,
                new TypeLiteral<String>() {},
                new TypeLiteral<Class<? extends TableHandle>>() {});

        return tableHandleTypes.addBinding(tableHandlerType);
    }

    private final BiMap<String, Class<? extends TableHandle>> tableHandleTypes;
    private final Map<Class<? extends TableHandle>, SimpleType> simpleTypes;

    @Inject
    public TableHandleJacksonModule(Map<String, Class<? extends TableHandle>> tableHandleTypes)
    {
        super(TableHandleJacksonModule.class.getSimpleName(), Version.unknownVersion());
        this.tableHandleTypes = ImmutableBiMap.copyOf(tableHandleTypes);

        ImmutableMap.Builder<Class<? extends TableHandle>, SimpleType> builder = ImmutableMap.builder();
        for (Class<? extends TableHandle> handleClass : this.tableHandleTypes.values()) {
            builder.put(handleClass, SimpleType.construct(handleClass));
        }
        this.simpleTypes = builder.build();

        addSerializer(TableHandle.class, new TableHandleSerializer(this));
        addDeserializer(TableHandle.class, new TableHandleDeserializer(this));
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
        String type = tableHandleTypes.inverse().get(suggestedType);
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
        Class<?> tableHandleClass = tableHandleTypes.get(id);
        checkState(tableHandleClass != null, "Type %s is unknown!", id);
        return simpleTypes.get(tableHandleClass);
    }

    @Override
    public Id getMechanism()
    {
        return Id.NAME;
    }

    public static class TableHandleDeserializer
            extends StdDeserializer<TableHandle>
    {
        private final TypeDeserializer typeDeserializer;

        @Inject
        public TableHandleDeserializer(@ForTableHandle TypeIdResolver typeIdResolver)
        {
            super(TableHandle.class);
            this.typeDeserializer = new AsPropertyTypeDeserializer(SimpleType.construct(TableHandle.class), typeIdResolver, "type", false, null);
        }

        @Override
        public TableHandle deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException
        {
            return (TableHandle) typeDeserializer.deserializeTypedFromAny(jp, ctxt);
        }
    }

    public static class TableHandleSerializer
            extends StdSerializer<TableHandle>
    {
        private final TypeSerializer typeSerializer;
        private final Cache<Class<? extends TableHandle>, JsonSerializer<Object>> serializerCache = CacheBuilder.newBuilder().build();

        @Inject
        public TableHandleSerializer(@ForTableHandle TypeIdResolver typeIdResolver)
        {
            super(TableHandle.class);
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, "type");
        }

        @Override
        public void serialize(final TableHandle value, JsonGenerator jgen, final SerializerProvider provider)
                throws IOException, JsonGenerationException
        {
            if (value == null) {
                provider.defaultSerializeNull(jgen);
            }
            else {
                try {
                    JsonSerializer<Object> serializer = serializerCache.get(value.getClass(), new Callable<JsonSerializer<Object>>() {

                        @Override
                        public JsonSerializer<Object> call()
                                throws Exception
                        {
                            return BeanSerializerFactory.instance.createSerializer(provider, provider.constructType(value.getClass()));
                        }

                    });

                    serializer.serializeWithType(value, jgen, provider, typeSerializer);
                }
                catch (ExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }
    }
}
