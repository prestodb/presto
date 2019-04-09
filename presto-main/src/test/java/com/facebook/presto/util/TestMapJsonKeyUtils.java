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

package com.facebook.presto.util;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.server.SliceDeserializer;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.TestingBlockEncodingSerde;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.util.MapJsonKeyUtils.bindMapKeySerde;
import static com.facebook.presto.util.MapJsonKeyUtils.createKeyDeserializer;
import static com.facebook.presto.util.MapJsonKeyUtils.createKeySerializer;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.testng.Assert.assertEquals;

public class TestMapJsonKeyUtils
{
    @Test
    public void testInjectedCodec()
            throws Exception
    {
        TestNode original = new TestNode(ImmutableMap.of(variable("a", BIGINT), constant(1L, BIGINT)));
        JsonCodec<TestNode> codec = getJsonCodec();
        assertEquals(codec.fromJson(codec.toJson(original)), original);
    }

    @Test
    public void testObjectMapper()
            throws Exception
    {
        TestNode original = new TestNode(ImmutableMap.of(variable("a", BIGINT), constant(1L, BIGINT)));
        ObjectMapper objectMapper = createObjectMapper();
        assertEquals(objectMapper.readValue(objectMapper.writeValueAsString(original), TestNode.class), original);
    }

    public static class TestNode
    {
        private Map<VariableReferenceExpression, ConstantExpression> values;

        @JsonCreator
        public TestNode(@JsonProperty Map<VariableReferenceExpression, ConstantExpression> values)
        {
            this.values = values;
        }

        @JsonProperty
        public Map<VariableReferenceExpression, ConstantExpression> getValues()
        {
            return values;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestNode)) {
                return false;
            }
            TestNode testNode = (TestNode) o;
            return Objects.equals(values, testNode.values);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(values);
        }
    }

    private JsonCodec<TestNode> getJsonCodec()
            throws Exception
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);

            binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            bindMapKeySerde(binder, VariableReferenceExpression.class);
            jsonCodecBinder(binder).bindJsonCodec(TestNode.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<TestNode>>() {});
    }

    private ObjectMapper createObjectMapper()
    {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        ObjectMapperProvider provider = new ObjectMapperProvider();
        BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde(metadata.getTypeManager());
        provider.setKeySerializers(ImmutableMap.of(
                VariableReferenceExpression.class, createKeySerializer(provider)));
        provider.setKeyDeserializers(ImmutableMap.of(
                VariableReferenceExpression.class, createKeyDeserializer(VariableReferenceExpression.class, provider)));
        provider.setJsonSerializers(ImmutableMap.of(
                Slice.class, new SliceSerializer(),
                Expression.class, new Serialization.ExpressionSerializer(),
                Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));
        provider.setJsonDeserializers(ImmutableMap.of(
                Slice.class, new SliceDeserializer(),
                Type.class, new TypeDeserializer(new TypeRegistry()),
                Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde)));
        return provider.get();
    }
}
