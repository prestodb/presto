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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.TableFunctionRegistry;
import com.facebook.presto.server.thrift.BlockCodec;
import com.facebook.presto.server.thrift.ConstantExpressionCodec;
import com.facebook.presto.server.thrift.TypeCodec;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.thrift.codec.ThriftCodecProvider;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

final class MetadataUtil
{
    private MetadataUtil() {}

    public static final JsonCodec<JdbcColumnHandle> COLUMN_JSON_CODEC;
    public static final JsonCodec<JdbcTableHandle> TABLE_JSON_CODEC;
    public static final JsonCodec<JdbcOutputTableHandle> OUTPUT_TABLE_JSON_CODEC;

    public static final ConnectorCodec<ColumnHandle> COLUMN_THRIFT_CODEC;
    public static final ConnectorCodec<ConnectorTableHandle> TABLE_THRIFT_CODEC;
    public static final ConnectorCodec<ConnectorOutputTableHandle> OUTPUT_TABLE_THRIFT_CODEC;
    public static final ConnectorCodec<ConnectorSplit> SPLIT_THRIFT_CODEC;

    static {
        JsonObjectMapperProvider jsonProvider = new JsonObjectMapperProvider();
        jsonProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(jsonProvider);
        COLUMN_JSON_CODEC = codecFactory.jsonCodec(JdbcColumnHandle.class);
        TABLE_JSON_CODEC = codecFactory.jsonCodec(JdbcTableHandle.class);
        OUTPUT_TABLE_JSON_CODEC = codecFactory.jsonCodec(JdbcOutputTableHandle.class);
        ThriftCodecManager manager = new ThriftCodecManager();
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        manager.addCodec(new BlockCodec(blockEncodingManager));
        FunctionAndTypeManager functionAndTypeManager = new FunctionAndTypeManager(
                createTestTransactionManager(),
                new TableFunctionRegistry(),
                blockEncodingManager,
                new FeaturesConfig(),
                new FunctionsConfig(),
                new HandleResolver(),
                ImmutableSet.of());
        manager.addCodec(new ConstantExpressionCodec(functionAndTypeManager, () -> manager));
        manager.addCodec(new TypeCodec(functionAndTypeManager));
        ThriftCodecProvider thriftProvider = new ThriftCodecProvider.Builder()
                .setThriftCodecManager(manager)
                .setConnectorSplitType(JdbcSplit.class)
                .setConnectorColumnHandle(JdbcColumnHandle.class)
                .setConnectorTableHandle(JdbcTableHandle.class)
                .setConnectorOutputTableHandle(JdbcOutputTableHandle.class).build();
        COLUMN_THRIFT_CODEC = thriftProvider.getColumnHandleCodec().get();
        TABLE_THRIFT_CODEC = thriftProvider.getConnectorTableHandleCodec().get();
        OUTPUT_TABLE_THRIFT_CODEC = thriftProvider.getConnectorOutputTableHandleCodec().get();
        SPLIT_THRIFT_CODEC = thriftProvider.getConnectorSplitCodec().get();
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.of(
                StandardTypes.BIGINT, BIGINT,
                StandardTypes.VARCHAR, VARCHAR);

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

    public static <T> void assertJsonRoundTrip(JsonCodec<T> codec, T object)
    {
        String json = codec.toJson(object);
        T copy = codec.fromJson(json);
        assertEquals(copy, object);
    }

    public static <T, Y> void assertThriftRoundTrip(ConnectorCodec<T> codec, Y object)
    {
        byte[] data = codec.serialize((T) object);
        T copy = codec.deserialize(data);
        assertEquals(copy, object);
    }
}
