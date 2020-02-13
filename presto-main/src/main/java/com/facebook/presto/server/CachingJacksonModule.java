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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A module which will cache serialization of objects of the specified type.
 */
class CachingJacksonModule
        extends Module
{
    private final Map<Class<?>, Function<?, ?>> classToFunctionKey;

    private CachingJacksonModule(Map<Class<?>, Function<?, ?>> classToFunctionKey)
    {
        this.classToFunctionKey = requireNonNull(classToFunctionKey, "classToFunctionKey is null");
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    @Override
    public String getModuleName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public Version version()
    {
        return Version.unknownVersion();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setupModule(SetupContext context)
    {
        context.addBeanSerializerModifier(new BeanSerializerModifier() {
            @Override
            public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer)
            {
                Class<?> beanClass = beanDesc.getBeanClass();
                if (classToFunctionKey.containsKey(beanClass)) {
                    return new CachingJacksonSerializer(beanClass, classToFunctionKey.get(beanClass), serializer, context.getOwner().getFactory());
                }
                return serializer;
            }
        });
    }

    public static final class Builder
    {
        private ImmutableMap.Builder mapBuilder = ImmutableMap.<Class<?>, Function<?, ?>>builder();

        @SuppressWarnings("unchecked")
        public <U, V> Builder withClass(Class<U> clazz, Function<U, V> keyingFunction)
        {
            mapBuilder.put(clazz, keyingFunction);
            return this;
        }

        @SuppressWarnings("unchecked")
        public <U> Builder withClass(Class<U> clazz)
        {
            return withClass(clazz, self -> self);
        }

        @SuppressWarnings("unchecked")
        public CachingJacksonModule build()
        {
            return new CachingJacksonModule(mapBuilder.build());
        }
    }
}
