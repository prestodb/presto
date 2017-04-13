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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.AllExpression;
import com.facebook.presto.spi.predicate.AndExpression;
import com.facebook.presto.spi.predicate.DomainExpression;
import com.facebook.presto.spi.predicate.NoneExpression;
import com.facebook.presto.spi.predicate.OrExpression;
import com.facebook.presto.spi.predicate.TupleExpression;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
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
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.util.Objects.requireNonNull;

public class TupleExpressionJacksonModule
        extends SimpleModule
{
    private static final String TYPE_PROPERTY = "@type";
    private static final Class baseClass = TupleExpression.class;

    @Inject
    public TupleExpressionJacksonModule(HandleResolver handleResolver)
    {
        super(baseClass.getSimpleName() + "Module", Version.unknownVersion());

        TypeIdResolver typeResolver = new InternalTypeResolver(handleResolver::getId,
                handleResolver::getColumnHandleClass, getExpressionResolver());

        addSerializer(baseClass, new InternalTypeSerializer(baseClass, typeResolver));
        addDeserializer(baseClass, new InternalTypeDeserializer(baseClass, typeResolver));
    }

    public Map<String, Class<? extends TupleExpression>> getExpressionResolver()
    {
        ImmutableMap.Builder<String, Class<? extends TupleExpression>> builder = ImmutableMap.builder();
        builder.put("all", AllExpression.class);
        builder.put("none", NoneExpression.class);
        builder.put("or", OrExpression.class);
        builder.put("and", AndExpression.class);
        builder.put("domain", DomainExpression.class);
        builder.put("not", NoneExpression.class);
        return builder.build();
    }

    private static class InternalTypeDeserializer
            extends StdDeserializer
    {
        private final TypeDeserializer typeDeserializer;

        public InternalTypeDeserializer(Class<TupleExpression> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            TypeFactory factory = TypeFactory.defaultInstance();
            this.typeDeserializer = new AsPropertyTypeDeserializer(TypeFactory.defaultInstance().constructParametricType(baseClass,
                    factory.constructType(ColumnHandle.class)), typeIdResolver, TYPE_PROPERTY, false, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public TupleExpression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {

            TupleExpression expression = (TupleExpression) (typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext));

            return expression;
        }
    }

    private static class InternalTypeSerializer
            extends StdSerializer
    {
        private final TypeSerializer typeSerializer;
        private final Cache<Class<?>, JsonSerializer> serializerCache = CacheBuilder.newBuilder().build();

        public InternalTypeSerializer(Class<TupleExpression> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, TYPE_PROPERTY);
        }

        @SuppressWarnings("unchecked")
        private static <T> JsonSerializer<T> createSerializer(SerializerProvider provider, Class<?> type)
                throws JsonMappingException
        {
            JavaType javaType = provider.getTypeFactory().constructParametricType(type, ColumnHandle.class);

            return (JsonSerializer<T>) BeanSerializerFactory.instance.createSerializer(provider, javaType);
        }

        @Override
        public void serialize(Object value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            if (value == null) {
                provider.defaultSerializeNull(generator);
                return;
            }

            try {
                Class<?> type = value.getClass();
                JsonSerializer serializer = serializerCache.get(type, () -> createSerializer(provider, type));
                serializer.serializeWithType(value, generator, provider, typeSerializer);
            }
            catch (ExecutionException e) {
                propagateIfInstanceOf(e.getCause(), IOException.class);
                throw Throwables.propagate(e.getCause());
            }
        }
    }

    private static class InternalTypeResolver
            extends TypeIdResolverBase
    {
        private final Function<ColumnHandle, String> nameResolver;
        private final Function<String, Class<? extends ColumnHandle>> classResolver;
        private final Map<String, Class<? extends TupleExpression>> expressionResolver;

        public InternalTypeResolver(Function<ColumnHandle, String> nameResolver, Function<String, Class<? extends ColumnHandle>> classResolver,
                Map<String, Class<? extends TupleExpression>> expressionResolver)
        {
            this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
            this.classResolver = requireNonNull(classResolver, "classResolver is null");
            this.expressionResolver = expressionResolver;
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
            if (value instanceof TupleExpression) {
                TupleExpression expression = (TupleExpression) value;
                if (expression instanceof DomainExpression) {

                    return expression.getName() + "@@" + nameResolver.apply((ColumnHandle) ((DomainExpression) expression).getColumn());
                }
                return expression.getName();
            }
            else {
                return null;
            }
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id)
        {
            requireNonNull(id, "id is null");
            if (id.split(".").length == 2) {
                String[] part = id.split("@@");
                Class<?> typeClass = expressionResolver.get(part[0]);
                Class<?> paramtericClass = classResolver.apply(part[1]);
                return context.getTypeFactory().constructParametricType(typeClass, paramtericClass);
            }
            else {
                Class<?> typeClass = expressionResolver.get(id);
                checkArgument(typeClass != null, "Unknown type ID: %s", id);
                return context.getTypeFactory().constructType(typeClass);
            }
        }

        @Override
        public JsonTypeInfo.Id getMechanism()
        {
            return JsonTypeInfo.Id.NAME;
        }
    }
}
