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
package com.facebook.presto.example;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;

public final class MetadataUtil
{
    private MetadataUtil()
    {
    }

    public static final JsonCodec<Map<String, List<ExampleTable>>> CATALOG_CODEC;
    public static final JsonCodec<ExampleTable> TABLE_CODEC;
    public static final JsonCodec<ExampleColumnHandle> COLUMN_CODEC;

    static {
        JsonObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        CATALOG_CODEC = codecFactory.mapJsonCodec(String.class, listJsonCodec(ExampleTable.class));
        TABLE_CODEC = codecFactory.jsonCodec(ExampleTable.class);
        COLUMN_CODEC = codecFactory.jsonCodec(ExampleColumnHandle.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<StandardTypes.Types, Type> types = ImmutableMap.of(
                StandardTypes.Types.BOOLEAN, BOOLEAN,
                StandardTypes.Types.BIGINT, BIGINT,
                StandardTypes.Types.INTEGER, INTEGER,
                StandardTypes.Types.DOUBLE, DOUBLE,
                StandardTypes.Types.VARCHAR, createUnboundedVarcharType());

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            StandardTypes.Types standardType = StandardTypes.Types.getTypeFromString(value.toLowerCase(ENGLISH));
            if (standardType == null) {
                throw new IllegalArgumentException("Unknown type " + value);
            }
            Type type = types.get(standardType);
            if (type == null) {
                throw new IllegalArgumentException("Unknown type " + value);
            }
            return type;
        }
    }
}
