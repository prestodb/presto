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

import com.facebook.airlift.log.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ThriftSerializationRegistry
{
    private static final Logger log = Logger.get(ThriftSerializationRegistry.class);

    private static final Map<String, Function<byte[], Object>> DESERIALIZERS = new HashMap<>();
    private static final Map<String, Function<Object, byte[]>> SERIALIZERS = new HashMap<>();

    private static final TSerializer serializer;
    private static final TDeserializer deserializer;

    static {
        try {
            serializer = new TSerializer(new TJSONProtocol.Factory());
            deserializer = new TDeserializer(new TJSONProtocol.Factory());
        }
        catch (TTransportException e) {
            log.error("Can not initialize thrift serde", e);
            throw new RuntimeException(e);
        }
    }

    public static <T, R> void registerSerializer(Class<T> clazz, @Nullable Function<T, R> toThrift, @Nullable Function<T, byte[]> ownSerializer)
    {
        if (SERIALIZERS.containsKey(clazz)) {
            throw new IllegalArgumentException("Type " + clazz + " is already registered");
        }
        if (ownSerializer != null) {
            SERIALIZERS.put(clazz.getName(), obj -> ownSerializer.apply((T) obj));
            return;
        }
        SERIALIZERS.put(clazz.getName(), obj -> {
            try {
                R thriftObj = toThrift.apply((T) obj);
                byte[] bytes = ThriftSerializationRegistry.serializer.serialize((TBase) thriftObj);
                return bytes;
            }
            catch (TException e) {
                throw new RuntimeException("Unable to serialize for " + clazz.getSimpleName(), e);
            }
        });
    }

    public static <T, R extends TBase> void registerDeserializer(Class<T> clazz, Class<R> thriftClazz, @Nullable Function<byte[], Object> ownDeserializer, @Nullable Function<R, T> ownConstructor)
    {
        String type = clazz.getName();
        if (DESERIALIZERS.containsKey(type)) {
            throw new IllegalArgumentException("Type " + type + " is already registered");
        }

        if (ownDeserializer != null) {
            DESERIALIZERS.put(type, ownDeserializer::apply);
            return;
        }

        DESERIALIZERS.put(type, bytes -> {
            try {
                Constructor<R> thriftConstructor = thriftClazz.getDeclaredConstructor();
                thriftConstructor.setAccessible(true);
                R thriftInstance = thriftConstructor.newInstance();
                deserializer.deserialize(thriftInstance, bytes);
                if (ownConstructor != null) {
                    return ownConstructor.apply(thriftInstance);
                }
                Constructor<T> constructor = clazz.getConstructor(thriftClazz);
                return constructor.newInstance(thriftInstance);
            }
            catch (NoSuchMethodException | TException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new RuntimeException("Unable to deserialize for " + type, e);
            }
        });
    }

    public static byte[] serialize(Object obj)
    {
        Function<Object, byte[]> serializer = SERIALIZERS.get(obj.getClass().getName());
        if (serializer == null) {
            throw new IllegalArgumentException("No serializer registered for " + obj.getClass().getSimpleName());
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
