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
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.thrift.codec.ThriftCodecProvider;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.SPLIT_THRIFT_CODEC;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.assertThriftRoundTrip;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestJdbcSplit
{
    private final JdbcSplit split = new JdbcSplit("connectorId", "catalog", "schemaName", "tableName", TupleDomain.all(), Optional.empty());

    @Test
    public void testNativeThriftSchema()
            throws IOException
    {
        Path basedir = Path.of(System.getProperty("basedir", "."));
        String generated = Files.readString(basedir.resolve("target/thrift/presto_jdbc.thrift"));
        String nativeSchema = Files.readString(basedir.resolve("../presto-native-execution/presto_cpp/main/thrift/presto_jdbc.thrift"));
        assertEquals(nativeSchema.substring(nativeSchema.indexOf("namespace cpp2")), generated);
    }

    @Test
    public void testAddresses()
    {
        // split uses "example" scheme so no addresses are available and is not remotely accessible
        assertEquals(split.getAddresses(), ImmutableList.of());
        assertEquals(split.getNodeSelectionStrategy(), NO_PREFERENCE);

        JdbcSplit jdbcSplit = new JdbcSplit("connectorId", "catalog", "schemaName", "tableName", TupleDomain.all(), Optional.empty());
        assertEquals(jdbcSplit.getAddresses(), ImmutableList.of());
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<JdbcSplit> codec = jsonCodec(JdbcSplit.class);
        assertJsonRoundTrip(codec, split);
    }

    @Test
    public void testThriftRoundTrip()
    {
        assertThriftRoundTrip(SPLIT_THRIFT_CODEC, (ConnectorSplit) split);
    }

    @Test
    public void testThriftPredicatesAndHandles()
    {
        ThriftCodecProvider provider = JdbcConnector.createCodecProvider(
                FunctionAndTypeManager.createTestFunctionAndTypeManager(), new BlockEncodingManager());
        JdbcColumnHandle column = new JdbcColumnHandle("connectorId", "column", new JdbcTypeHandle(java.sql.Types.BIGINT, "bigint", 19, 0), BIGINT, true, Optional.of("column description"));
        List<TupleDomain<ColumnHandle>> domains = ImmutableList.of(
                TupleDomain.all(),
                TupleDomain.none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.onlyNull(BIGINT))),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.notNull(BIGINT))),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.singleValue(BIGINT, 42L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.create(ValueSet.ofRanges(
                        Range.lessThan(BIGINT, -10L), Range.range(BIGINT, 1L, true, 10L, false), Range.greaterThan(BIGINT, 100L)), true))));
        JdbcExpression predicate = new JdbcExpression("a = ? AND b = ? AND c = ? AND d = ?", ImmutableList.of(
                new ConstantExpression(42L, BIGINT),
                new ConstantExpression(utf8Slice("hello"), VARCHAR),
                new ConstantExpression(null, BIGINT),
                new ConstantExpression(true, BOOLEAN)));
        JdbcTableHandle table = new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), null, null, "table");
        for (TupleDomain<ColumnHandle> domain : domains) {
            for (Optional<JdbcExpression> additionalPredicate : ImmutableList.of(Optional.<JdbcExpression>empty(), Optional.of(predicate))) {
                JdbcSplit original = new JdbcSplit("connectorId", null, null, "table", domain, additionalPredicate);
                assertThriftRoundTrip(provider.getConnectorSplitCodec().get(), original);
                JdbcTableLayoutHandle layout = new JdbcTableLayoutHandle(table, domain, additionalPredicate, "layout");
                JdbcTableLayoutHandle copy = (JdbcTableLayoutHandle) provider.getConnectorTableLayoutHandleCodec().get()
                        .deserialize(provider.getConnectorTableLayoutHandleCodec().get().serialize(layout));
                assertEquals(copy.getTable(), table);
                assertEquals(copy.getTupleDomain(), domain);
                assertEquals(copy.getAdditionalPredicate(), additionalPredicate);
                assertEquals(copy.getLayoutString(), "layout");
            }
        }
        assertThriftRoundTrip(provider.getConnectorTableHandleCodec().get(), table);
        assertThriftRoundTrip(provider.getColumnHandleCodec().get(), column);
        assertThriftRoundTrip(provider.getConnectorTransactionHandleCodec().get(), new JdbcTransactionHandle());
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, createDecimalType(18, 4));
        JdbcOutputTableHandle output = new JdbcOutputTableHandle("connectorId", null, null, "table",
                ImmutableList.of("a", "b", "c"), types, "temporary");
        assertThriftRoundTrip(provider.getConnectorOutputTableHandleCodec().get(), output);
        assertThriftRoundTrip(provider.getConnectorInsertTableHandleCodec().get(), output);
    }

    @Test
    public void testThriftValueSets()
    {
        ThriftCodecProvider provider = JdbcConnector.createCodecProvider(
                FunctionAndTypeManager.createTestFunctionAndTypeManager(), new BlockEncodingManager());
        for (Domain domain : ImmutableList.of(
                Domain.singleValue(JSON, utf8Slice("{\"a\":1}")),
                Domain.create(ValueSet.of(JSON, utf8Slice("1"), utf8Slice("2")).complement(), true),
                Domain.notNull(HYPER_LOG_LOG),
                Domain.onlyNull(HYPER_LOG_LOG))) {
            JdbcColumnHandle column = new JdbcColumnHandle("connectorId", "column", new JdbcTypeHandle(java.sql.Types.OTHER, "other", 0, 0), domain.getType(), true, Optional.empty());
            JdbcSplit original = new JdbcSplit("connectorId", "catalog", "schema", "table",
                    TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)), Optional.empty());
            assertThriftRoundTrip(provider.getConnectorSplitCodec().get(), original);
        }
    }

    @Test
    public void testNativeThriftFixtures()
            throws Exception
    {
        FunctionAndTypeManager typeManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        ThriftCodecProvider provider = JdbcConnector.createCodecProvider(typeManager, blockEncodingManager);
        JsonObjectMapperProvider jsonProvider = new JsonObjectMapperProvider();
        jsonProvider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(typeManager),
                Block.class, new BlockJsonSerde.Deserializer(blockEncodingManager),
                ColumnHandle.class, new JsonDeserializer<ColumnHandle>()
                {
                    @Override
                    public ColumnHandle deserialize(JsonParser parser, DeserializationContext context)
                            throws IOException
                    {
                        return parser.readValueAs(JdbcColumnHandle.class);
                    }
                }));
        jsonProvider.setJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingManager)));
        ObjectMapper mapper = jsonProvider.get();
        try (InputStream input = getClass().getResourceAsStream("/jdbc-thrift.json")) {
            for (JsonNode fixture : mapper.readTree(input)) {
                switch (fixture.get("kind").asText()) {
                    case "column":
                        assertNativeFixture(mapper, fixture, JdbcColumnHandle.class, provider.getColumnHandleCodec().get());
                        break;
                    case "table":
                        assertNativeFixture(mapper, fixture, JdbcTableHandle.class, provider.getConnectorTableHandleCodec().get());
                        break;
                    case "transaction":
                        assertNativeFixture(mapper, fixture, JdbcTransactionHandle.class, provider.getConnectorTransactionHandleCodec().get());
                        break;
                    case "layout":
                        assertNativeFixture(mapper, fixture, JdbcTableLayoutHandle.class, provider.getConnectorTableLayoutHandleCodec().get());
                        break;
                    case "split":
                        assertNativeFixture(mapper, fixture, JdbcSplit.class, provider.getConnectorSplitCodec().get());
                        break;
                    default:
                        throw new AssertionError("Unknown fixture kind: " + fixture.get("kind"));
                }
            }
        }
    }

    private static <T> void assertNativeFixture(ObjectMapper mapper, JsonNode fixture, Class<? extends T> type, ConnectorCodec<T> codec)
            throws IOException
    {
        T original = mapper.treeToValue(fixture.get("json"), type);
        byte[] bytes = Base64.getDecoder().decode(fixture.get("thrift").asText());
        assertEquals(codec.deserialize(codec.serialize(original)), original);
        T copy = codec.deserialize(bytes);
        assertEquals(copy, original);
    }
}
