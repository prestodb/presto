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
package com.facebook.presto.hive;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.CacheQuotaRequirement.NO_CACHE_REQUIREMENT;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;

public class TestHiveSplit
{
    @Test
    public void testJsonRoundTrip()
            throws Exception
    {
        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("a", Optional.of("apple")), new HivePartitionKey("b", Optional.of("42")));
        ImmutableList<HostAddress> addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 44), HostAddress.fromParts("127.0.0.1", 45));
        Map<String, String> customSplitInfo = ImmutableMap.of("key", "value");
        Set<ColumnHandle> redundantColumnDomains = ImmutableSet.of(new HiveColumnHandle(
                "test_column",
                HIVE_LONG,
                HIVE_LONG.getTypeSignature(),
                5,
                REGULAR,
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty()));
        HiveSplit expected = new HiveSplit(
                "db",
                "table",
                "partitionId",
                "path",
                42,
                87,
                88,
                Instant.now().toEpochMilli(),
                new Storage(
                        StorageFormat.create("serde", "input", "output"),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                partitionKeys,
                addresses,
                OptionalInt.empty(),
                OptionalInt.empty(),
                NO_PREFERENCE,
                10,
                TableToPartitionMapping.mapColumnsByIndex(ImmutableMap.of(1, new Column("name", HIVE_STRING, Optional.empty()))),
                Optional.of(new HiveSplit.BucketConversion(
                        32,
                        16,
                        ImmutableList.of(new HiveColumnHandle("col", HIVE_LONG, BIGINT.getTypeSignature(), 5, REGULAR, Optional.of("comment"), Optional.empty())))),
                false,
                Optional.empty(),
                NO_CACHE_REQUIREMENT,
                Optional.of(EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(
                        ImmutableMap.of("field1", "test1".getBytes()),
                        ImmutableMap.of(),
                        "test_algo",
                        "test_provider"))),
                customSplitInfo,
                redundantColumnDomains);

        JsonCodec<HiveSplit> codec = getJsonCodec();
        String json = codec.toJson(expected);
        HiveSplit actual = codec.fromJson(json);

        assertEquals(actual.getDatabase(), expected.getDatabase());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getPartitionName(), expected.getPartitionName());
        assertEquals(actual.getPath(), expected.getPath());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getFileSize(), expected.getFileSize());
        assertEquals(actual.getStorage(), expected.getStorage());
        assertEquals(actual.getPartitionKeys(), expected.getPartitionKeys());
        assertEquals(actual.getAddresses(), expected.getAddresses());
        assertEquals(actual.getPartitionDataColumnCount(), expected.getPartitionDataColumnCount());
        assertEquals(actual.getTableToPartitionMapping().getPartitionSchemaDifference(), expected.getTableToPartitionMapping().getPartitionSchemaDifference());
        assertEquals(actual.getTableToPartitionMapping().getTableToPartitionColumns(), expected.getTableToPartitionMapping().getTableToPartitionColumns());
        assertEquals(actual.getBucketConversion(), expected.getBucketConversion());
        assertEquals(actual.getNodeSelectionStrategy(), expected.getNodeSelectionStrategy());
        assertEquals(actual.isS3SelectPushdownEnabled(), expected.isS3SelectPushdownEnabled());
        assertEquals(actual.getCacheQuotaRequirement(), expected.getCacheQuotaRequirement());
        assertEquals(actual.getEncryptionInformation(), expected.getEncryptionInformation());
        assertEquals(actual.getCustomSplitInfo(), expected.getCustomSplitInfo());
    }

    private JsonCodec<HiveSplit> getJsonCodec()
            throws Exception
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
            jsonCodecBinder(binder).bindJsonCodec(HiveSplit.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("hive", new HiveHandleResolver());
        return injector.getInstance(new Key<JsonCodec<HiveSplit>>() {});
    }
}
