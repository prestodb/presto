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
package com.facebook.presto.memory;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.DataSize;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMemoryInfo
{
    private static final JsonCodec<MemoryInfo> CODEC = jsonCodec(MemoryInfo.class);

    @Test
    public void testJsonRoundTrip()
    {
        // Non-zero sentinel so a dropped or swapped field would be caught.
        MemoryInfo expected = new MemoryInfo(new DataSize(4, GIGABYTE), 987_654_321L, ImmutableMap.of());

        MemoryInfo actual = CODEC.fromJson(CODEC.toJson(expected));

        assertEquals(actual.getSystemMemoryBytes(), 987_654_321L);
        assertEquals(actual.getTotalNodeMemory(), expected.getTotalNodeMemory());
        assertEquals(actual.getPools(), expected.getPools());
    }

    @Test
    public void testTwoArgConstructorDefaultsSystemMemoryBytesToZero()
    {
        // The legacy 2-arg constructor must keep working — many callers (and
        // all existing MemoryInfo call sites outside of Prestissimo) use it.
        MemoryInfo info = new MemoryInfo(new DataSize(4, GIGABYTE), ImmutableMap.of());

        assertEquals(info.getSystemMemoryBytes(), 0L);
    }

    @Test
    public void testDeserializeLegacyPayloadWithoutSystemMemoryBytes()
    {
        // A pre-upgrade worker never emits systemMemoryBytes. The new coordinator
        // must still deserialize its MemoryInfo, defaulting the missing field
        // to 0 (mirrors the native from_json presence guard).
        String legacyJson = "{\"totalNodeMemory\":\"4GB\",\"pools\":{}}";

        MemoryInfo actual = CODEC.fromJson(legacyJson);

        assertEquals(actual.getSystemMemoryBytes(), 0L);
        assertEquals(actual.getTotalNodeMemory(), new DataSize(4, GIGABYTE));
    }

    @Test
    public void testSerializedPayloadIncludesSystemMemoryBytes()
            throws Exception
    {
        // Even when systemMemoryBytes is 0, the field must appear in the
        // serialized JSON so a downstream reader can distinguish "upgraded
        // worker reporting zero" from "legacy worker missing the field".
        // Parse the output back as a tree so this test also catches a
        // serializer that emits a wrong non-zero value for a zero input.
        MemoryInfo info = new MemoryInfo(new DataSize(4, GIGABYTE), 0L, ImmutableMap.of());

        String json = CODEC.toJson(info);
        JsonNode node = new ObjectMapper().readTree(json);

        assertTrue(node.has("systemMemoryBytes"), "systemMemoryBytes missing from JSON: " + json);
        assertEquals(node.get("systemMemoryBytes").asLong(), 0L);
    }
}
