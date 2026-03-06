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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for DynamicFilterResponse JSON serialization/deserialization.
 * Verifies that DynamicFilterResponse correctly round-trips through JSON,
 * which is essential for HTTP transport between coordinator and workers.
 */
public class TestDynamicFilterResponse
{
    private JsonCodec<DynamicFilterResponse> codec;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                binder -> {
                    FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
                    binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
                    binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class);
                    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                    jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
                    jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
                    newSetBinder(binder, Type.class);
                    jsonCodecBinder(binder).bindJsonCodec(DynamicFilterResponse.class);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        codec = injector.getInstance(new Key<JsonCodec<DynamicFilterResponse>>() {});
    }

    @Test
    public void testEmptyFilters()
    {
        DynamicFilterResponse original = DynamicFilterResponse.incomplete(ImmutableMap.of(), 1L);

        String json = codec.toJson(original);
        DynamicFilterResponse deserialized = codec.fromJson(json);

        assertEquals(deserialized.getFilters().size(), 0);
        assertEquals(deserialized.getVersion(), 1L);
    }

    @Test
    public void testSingleFilter()
    {
        TupleDomain<String> filter = TupleDomain.withColumnDomains(
                ImmutableMap.of("col1", Domain.singleValue(BIGINT, 42L)));
        DynamicFilterResponse original = DynamicFilterResponse.incomplete(
                ImmutableMap.of("filter1", filter), 2L);

        String json = codec.toJson(original);
        DynamicFilterResponse deserialized = codec.fromJson(json);

        assertEquals(deserialized.getFilters().size(), 1);
        assertEquals(deserialized.getVersion(), 2L);
        assertTrue(deserialized.getFilters().containsKey("filter1"));

        TupleDomain<String> deserializedFilter = deserialized.getFilters().get("filter1");
        assertNotNull(deserializedFilter);
        assertTrue(deserializedFilter.getDomains().isPresent());
        assertEquals(deserializedFilter.getDomains().get().size(), 1);
    }

    @Test
    public void testMultipleFilters()
    {
        TupleDomain<String> filter1 = TupleDomain.withColumnDomains(
                ImmutableMap.of("col1", Domain.singleValue(BIGINT, 1L)));
        TupleDomain<String> filter2 = TupleDomain.withColumnDomains(
                ImmutableMap.of("col2", Domain.singleValue(VARCHAR, utf8Slice("value"))));
        TupleDomain<String> filter3 = TupleDomain.withColumnDomains(
                ImmutableMap.of("col3", Domain.singleValue(BIGINT, 100L)));

        DynamicFilterResponse original = DynamicFilterResponse.incomplete(
                ImmutableMap.of("filter1", filter1, "filter2", filter2, "filter3", filter3), 5L);

        String json = codec.toJson(original);
        DynamicFilterResponse deserialized = codec.fromJson(json);

        assertEquals(deserialized.getFilters().size(), 3);
        assertEquals(deserialized.getVersion(), 5L);
        assertTrue(deserialized.getFilters().containsKey("filter1"));
        assertTrue(deserialized.getFilters().containsKey("filter2"));
        assertTrue(deserialized.getFilters().containsKey("filter3"));
    }

    @Test
    public void testVersionField()
    {
        DynamicFilterResponse response1 = DynamicFilterResponse.incomplete(ImmutableMap.of(), 0L);
        DynamicFilterResponse response2 = DynamicFilterResponse.incomplete(ImmutableMap.of(), Long.MAX_VALUE);

        DynamicFilterResponse deserialized1 = codec.fromJson(codec.toJson(response1));
        DynamicFilterResponse deserialized2 = codec.fromJson(codec.toJson(response2));

        assertEquals(deserialized1.getVersion(), 0L);
        assertEquals(deserialized2.getVersion(), Long.MAX_VALUE);
    }

    @Test
    public void testTupleDomainAll()
    {
        TupleDomain<String> filter = TupleDomain.all();
        DynamicFilterResponse original = DynamicFilterResponse.incomplete(
                ImmutableMap.of("allFilter", filter), 3L);

        String json = codec.toJson(original);
        DynamicFilterResponse deserialized = codec.fromJson(json);

        TupleDomain<String> deserializedFilter = deserialized.getFilters().get("allFilter");
        assertNotNull(deserializedFilter);
        assertTrue(deserializedFilter.isAll());
    }

    @Test
    public void testTupleDomainNone()
    {
        TupleDomain<String> filter = TupleDomain.none();
        DynamicFilterResponse original = DynamicFilterResponse.incomplete(
                ImmutableMap.of("noneFilter", filter), 4L);

        String json = codec.toJson(original);
        DynamicFilterResponse deserialized = codec.fromJson(json);

        TupleDomain<String> deserializedFilter = deserialized.getFilters().get("noneFilter");
        assertNotNull(deserializedFilter);
        assertTrue(deserializedFilter.isNone());
    }

    @Test
    public void testTupleDomainWithDomains()
    {
        // Test with multiple columns and different domain types
        Map<String, Domain> domains = ImmutableMap.of(
                "bigint_col", Domain.singleValue(BIGINT, 123L),
                "varchar_col", Domain.singleValue(VARCHAR, utf8Slice("test_value")));
        TupleDomain<String> filter = TupleDomain.withColumnDomains(domains);
        DynamicFilterResponse original = DynamicFilterResponse.incomplete(
                ImmutableMap.of("complexFilter", filter), 6L);

        String json = codec.toJson(original);
        DynamicFilterResponse deserialized = codec.fromJson(json);

        TupleDomain<String> deserializedFilter = deserialized.getFilters().get("complexFilter");
        assertNotNull(deserializedFilter);
        assertTrue(deserializedFilter.getDomains().isPresent());
        assertEquals(deserializedFilter.getDomains().get().size(), 2);

        // Verify the domains preserved their values
        Domain bigintDomain = deserializedFilter.getDomains().get().get("bigint_col");
        Domain varcharDomain = deserializedFilter.getDomains().get().get("varchar_col");
        assertNotNull(bigintDomain);
        assertNotNull(varcharDomain);
        assertTrue(bigintDomain.isSingleValue());
        assertTrue(varcharDomain.isSingleValue());
        assertEquals(bigintDomain.getSingleValue(), 123L);
        assertEquals(varcharDomain.getSingleValue(), utf8Slice("test_value"));
    }
}
