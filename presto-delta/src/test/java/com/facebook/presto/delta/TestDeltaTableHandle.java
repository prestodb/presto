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
package com.facebook.presto.delta;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.StandardTypes.DATE;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.Float.floatToRawIntBits;
import static org.testng.Assert.assertEquals;

/**
 * Test {@link DeltaTableHandle} is created correctly with given arguments and JSON serialization/deserialization works.
 */
public class TestDeltaTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        List<DeltaColumn> columns = ImmutableList.of(
                new DeltaColumn("c1", parseTypeSignature(StandardTypes.REAL), true, true),
                new DeltaColumn("c2", parseTypeSignature(INTEGER), false, true),
                new DeltaColumn("c3", parseTypeSignature(DOUBLE), false, false),
                new DeltaColumn("c4", parseTypeSignature(DATE), true, false));

        DeltaTable deltaTable = new DeltaTable(
                "schema",
                "table",
                "s3:/bucket/table/location",
                Optional.of(1L),
                columns);

        DeltaColumnHandle c1ColumnHandle = new DeltaColumnHandle(
                columns.get(0).getName(),
                columns.get(0).getType(),
                columns.get(0).isPartition() ? PARTITION : REGULAR,
                Optional.empty());

        TupleDomain<DeltaColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                c1ColumnHandle, Domain.create(SortedRangeSet.copyOf(REAL,
                                ImmutableList.of(
                                        Range.equal(REAL, (long) floatToRawIntBits(100.0f + 0)),
                                        Range.equal(REAL, (long) floatToRawIntBits(100.008f + 0)),
                                        Range.equal(REAL, (long) floatToRawIntBits(100.0f + 14)))),
                        false)));

        DeltaTableHandle expected = new DeltaTableHandle("delta", deltaTable);

        String json = getJsonCodec().toJson(expected);
        DeltaTableHandle actual = getJsonCodec().fromJson(json);

        assertEquals(actual.getDeltaTable(), expected.getDeltaTable());
    }

    private JsonCodec<DeltaTableHandle> getJsonCodec()
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(DeltaTableHandle.class);
            jsonCodecBinder(binder).bindJsonCodec(DeltaTable.class);
            jsonCodecBinder(binder).bindJsonCodec(DeltaColumnHandle.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("delta", new DeltaConnectionHandleResolver());
        return injector.getInstance(new Key<JsonCodec<DeltaTableHandle>>() {});
    }
}
