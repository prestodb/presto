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
package io.prestosql.plugin.redis.util;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;

import java.util.function.Supplier;

import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

public final class CodecSupplier<T>
        implements Supplier<JsonCodec<T>>
{
    private final JsonCodecFactory codecFactory;
    private final Class<T> clazz;

    public CodecSupplier(Class<T> clazz, Metadata metadata)
    {
        this.clazz = clazz;
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(metadata)));
        this.codecFactory = new JsonCodecFactory(objectMapperProvider);
    }

    @Override
    public JsonCodec<T> get()
    {
        return codecFactory.jsonCodec(clazz);
    }

    private static class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;
        private final Metadata metadata;

        public TypeDeserializer(Metadata metadata)
        {
            super(Type.class);
            this.metadata = metadata;
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = metadata.getType(parseTypeSignature(value));
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }
}
