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

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestHiveColumnHandle
{
    private final JsonCodec<HiveColumnHandle> codec = JsonCodec.jsonCodec(HiveColumnHandle.class);

    @Test
    public void testRoundTrip()
    {
        HiveColumnHandle expected = new HiveColumnHandle("client", "name", 42, HiveType.FLOAT, 88, true);

        String json = codec.toJson(expected);
        HiveColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getClientId(), expected.getClientId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getHiveType(), expected.getHiveType());
        assertEquals(actual.getHiveColumnIndex(), expected.getHiveColumnIndex());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
    }
}
