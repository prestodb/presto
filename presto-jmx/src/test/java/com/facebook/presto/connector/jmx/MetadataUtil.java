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
package com.facebook.presto.connector.jmx;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static final JsonCodec<JmxTableHandle> TABLE_CODEC;
    public static final JsonCodec<JmxColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<JmxSplit> SPLIT_CODEC;

    static {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        TABLE_CODEC = codecFactory.jsonCodec(JmxTableHandle.class);
        COLUMN_CODEC = codecFactory.jsonCodec(JmxColumnHandle.class);
        SPLIT_CODEC = codecFactory.jsonCodec(JmxSplit.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.<String, Type>builder()
                .put(StandardTypes.BIGINT, BIGINT)
                .put(StandardTypes.VARCHAR, createUnboundedVarcharType()) // with max value length in signature
                .build();

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(ENGLISH));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
