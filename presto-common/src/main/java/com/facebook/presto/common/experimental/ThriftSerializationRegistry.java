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
package com.facebook.presto.common.experimental;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ThriftSerializationRegistry
{
    private ThriftSerializationRegistry() {}

    private static final Map<String, Function<byte[], Object>> DESERIALIZERS = new HashMap<>();
    private static final Map<Class<?>, Function<Object, byte[]>> SERIALIZERS = new HashMap<>();

    public static <T> void registerDeserializer(String type, Function<byte[], T> deserializer)
    {
        if (DESERIALIZERS.containsKey(type)) {
            throw new IllegalArgumentException("Type " + type + " is already registered");
        }
        DESERIALIZERS.put(type, bytes -> deserializer.apply(bytes));
    }

    public static <T> void registerSerializer(Class<T> clazz, Function<T, byte[]> serializer)
    {
        if (SERIALIZERS.containsKey(clazz)) {
            throw new IllegalArgumentException("Type " + clazz + " is already registered");
        }
        SERIALIZERS.put(clazz, obj -> serializer.apply((T) obj));
    }

    public static byte[] serialize(Object obj)
    {
        Function<Object, byte[]> serializer = SERIALIZERS.get(obj.getClass());
        if (serializer == null) {
            throw new IllegalArgumentException("No serializer registered for " + obj.getClass());
        }
        return serializer.apply(obj);
    }

    public static Object deserialize(String type, byte[] bytes)
    {
        Function<byte[], Object> deserializer = DESERIALIZERS.get(type);
        if (deserializer == null) {
            throw new IllegalArgumentException("No deserializer registered for " + type);
        }
        return deserializer.apply(bytes);
    }
}
