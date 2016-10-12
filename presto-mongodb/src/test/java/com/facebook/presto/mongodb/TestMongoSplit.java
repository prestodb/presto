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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestMongoSplit
{
    private final JsonCodec<MongoSplit> codec = JsonCodec.jsonCodec(MongoSplit.class);

    @Test
    public void testJsonRoundTrip()
    {
        MongoSplit expected = new MongoSplit(new SchemaTableName("schema1", "table1"), TupleDomain.all(), ImmutableList.of());

        String json = codec.toJson(expected);
        MongoSplit actual = codec.fromJson(json);

        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
        assertEquals(actual.getTupleDomain(), TupleDomain.<ColumnHandle>all());
        assertEquals(actual.getAddresses(), ImmutableList.of());
    }
}
