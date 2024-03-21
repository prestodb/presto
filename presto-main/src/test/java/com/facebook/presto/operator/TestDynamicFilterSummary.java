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
package com.facebook.presto.operator;

import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.bloomfilter.BloomFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilterImpl;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterSummary
{
    private ObjectMapper mapper = getObjectMapperProvider().get();

    private ObjectMapperProvider getObjectMapperProvider()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        ImmutableMap.Builder deserializers = ImmutableMap.builder();
        BlockEncodingManager blockEncodingSerde = new BlockEncodingManager();
        deserializers.put(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde));
        deserializers.put(Type.class, new TypeDeserializer(FunctionAndTypeManager.createTestFunctionAndTypeManager()));
        provider.setJsonDeserializers(deserializers.build());
        provider.setJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));
        return provider;
    }

    @Test
    public void testDynamicFilterSummarySerialization() throws Exception
    {
        BloomFilterForDynamicFilter alice = new BloomFilterForDynamicFilterImpl(BloomFilter.create(1024, 0.03), null, 0);
        BloomFilterForDynamicFilter bob = new BloomFilterForDynamicFilterImpl(BloomFilter.create(1024, 0.03), null, 0);
        alice.put("123");
        alice.put("456");
        bob.put("234");
        bob.put("345");

        DynamicFilterSummary aliceSummary = new DynamicFilterSummary(alice);
        DynamicFilterSummary bobSummary = new DynamicFilterSummary(bob);
        DynamicFilterSummary summary = aliceSummary.mergeWith(bobSummary);

        String serializedJson = mapper.writeValueAsString(summary);
        System.out.println(serializedJson);
        DynamicFilterSummary deserialSummary = mapper.readValue(serializedJson, DynamicFilterSummary.class);

        assertTrue(deserialSummary.getBloomFilterForDynamicFilter().contain("123"));
        assertTrue(deserialSummary.getBloomFilterForDynamicFilter().contain("234"));
        assertTrue(deserialSummary.getBloomFilterForDynamicFilter().contain("456"));
        assertFalse(deserialSummary.getBloomFilterForDynamicFilter().contain("1234"));
    }

    @Test
    public void testDynamicFilterSummaryDeserialization() throws Exception
    {
        String json = "{\"bloomFilterForDynamicFilter\":{\"@type\":\"bloomFilter\",\"bloomFilter\":{\"@type\":\"BloomFilterImpl\"},\"compressedBloomFilter\":\"KLUv/WC1Au0CACQCAAAAAQAAAAAFAAAAdQBAAAGAABAABAAgAAgABBAIAgIgCBgA4BNwHPDMVBhzKwY2MAUoDzxOXZ4BZHAlACwMMAYGBgzGAArBCRWyUrAABQgDAoEJcn4XKGwASg==\",\"sizeBeforeCompressed\":949,\"sizeAfterCompressed\":103}}";
        DynamicFilterSummary deserialSummary = mapper.readValue(json, DynamicFilterSummary.class);
        String serializedJson = mapper.writeValueAsString(deserialSummary);
        assertEquals(json, serializedJson);
    }
}
