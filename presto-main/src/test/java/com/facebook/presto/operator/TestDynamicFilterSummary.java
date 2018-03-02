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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestDynamicFilterSummary
{
    private ObjectMapper mapper = getObjectMapperProvider().get();

    private ObjectMapperProvider getObjectMapperProvider()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        ImmutableMap.Builder deserializers = ImmutableMap.builder();
        BlockEncodingManager blockEncodingSerde = new BlockEncodingManager(new TypeRegistry());
        deserializers.put(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde));
        deserializers.put(Type.class, new TypeDeserializer(new TypeRegistry()));
        provider.setJsonDeserializers(deserializers.build());
        provider.setJsonSerializers(ImmutableMap.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)));
        return provider;
    }

    @Test
    public void testDFSerialization() throws Exception
    {
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<String, Domain>builder()
                        .put("A", Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put("B", Domain.singleValue(DOUBLE, 0.0))
                        .put("C", Domain.singleValue(BIGINT, 1L))
                        .put("D", Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 0.0, true, 10.0, false)), false))
                        .build());
        DynamicFilterSummary summary = new DynamicFilterSummary(tupleDomain);
        String serializedJson = mapper.writeValueAsString(summary);
        DynamicFilterSummary deserialSummary = mapper.readValue(serializedJson, DynamicFilterSummary.class);
        assertEquals(summary, deserialSummary);
    }

    @Test
    public void testDFDeserialization() throws Exception
    {
        String json = "{\"tupleDomain\":{\"columnDomains\":[{\"column\":\"df\",\"domain\":{\"values\":{\"@type\":\"sortable\",\"type\":\"varchar\",\"ranges\":[{\"low\":{\"type\":\"varchar\",\"valueBlock\":\"DgAAAFZBUklBQkxFX1dJRFRIAQAAAAQAAAAABAAAADIwMTc=\",\"bound\":\"EXACTLY\"},\"high\":{\"type\":\"varchar\",\"valueBlock\":\"DgAAAFZBUklBQkxFX1dJRFRIAQAAAAQAAAAABAAAADIwMTc=\",\"bound\":\"EXACTLY\"}}]},\"nullAllowed\":false}}]}}";
        DynamicFilterSummary deserialSummary = mapper.readValue(json, DynamicFilterSummary.class);
        String serializedJson = mapper.writeValueAsString(deserialSummary);
        assertEquals(json, serializedJson);
    }
}
