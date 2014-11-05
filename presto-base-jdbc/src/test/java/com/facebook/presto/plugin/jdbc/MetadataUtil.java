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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

final class MetadataUtil
{
    private MetadataUtil() {}

    public static final JsonCodec<JdbcColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<JdbcTableHandle> TABLE_CODEC;
    public static final JsonCodec<JdbcOutputTableHandle> OUTPUT_TABLE_CODEC;

    static {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        COLUMN_CODEC = codecFactory.jsonCodec(JdbcColumnHandle.class);
        TABLE_CODEC = codecFactory.jsonCodec(JdbcTableHandle.class);
        OUTPUT_TABLE_CODEC = codecFactory.jsonCodec(JdbcOutputTableHandle.class);
    }

    public static final class TestingTypeDeserializer
            extends StdDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.<String, Type>of(
                StandardTypes.BIGINT, BIGINT,
                StandardTypes.VARCHAR, VARCHAR);

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        public Type deserialize(JsonParser jsonParser, DeserializationContext context)
                throws IOException
        {
            TypeSignature typeSignature = jsonParser.readValueAs(TypeSignature.class);
            Type type = types.get(typeSignature.toString());
            checkArgument(type != null, "Unknown type %s", typeSignature);
            return type;
        }
    }

    public static <T> void assertJsonRoundTrip(JsonCodec<T> codec, T object)
    {
        String json = codec.toJson(object);
        T copy = codec.fromJson(json);
        assertEquals(copy, object);
    }
}
