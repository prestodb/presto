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
package com.facebook.presto.localfile;

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
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;

final class MetadataUtil
{
    private MetadataUtil()
    {
    }

    public static final JsonCodec<LocalFileColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<LocalFileTableHandle> TABLE_CODEC;

    static {
        JsonObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        COLUMN_CODEC = codecFactory.jsonCodec(LocalFileColumnHandle.class);
        TABLE_CODEC = codecFactory.jsonCodec(LocalFileTableHandle.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = new ImmutableMap.Builder<String, Type>()
                .put(StandardTypes.BOOLEAN, BOOLEAN)
                .put(StandardTypes.BIGINT, BIGINT)
                .put(StandardTypes.DOUBLE, DOUBLE)
                .put(StandardTypes.TIMESTAMP, TIMESTAMP)
                .put(StandardTypes.DATE, DATE)
                .put(StandardTypes.VARCHAR, createUnboundedVarcharType())
                .build();

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(ENGLISH));
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }
}
