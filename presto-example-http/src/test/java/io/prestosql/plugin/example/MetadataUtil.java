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
package io.prestosql.plugin.example;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;

import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
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
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        CATALOG_CODEC = codecFactory.mapJsonCodec(String.class, listJsonCodec(ExampleTable.class));
        TABLE_CODEC = codecFactory.jsonCodec(ExampleTable.class);
        COLUMN_CODEC = codecFactory.jsonCodec(ExampleColumnHandle.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.of(
                StandardTypes.BOOLEAN, BOOLEAN,
                StandardTypes.BIGINT, BIGINT,
                StandardTypes.INTEGER, INTEGER,
                StandardTypes.DOUBLE, DOUBLE,
                StandardTypes.VARCHAR, createUnboundedVarcharType());

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
