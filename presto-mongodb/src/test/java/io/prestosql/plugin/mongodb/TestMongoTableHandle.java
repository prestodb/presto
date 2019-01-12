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

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestMongoTableHandle
{
    private final JsonCodec<MongoTableHandle> codec = JsonCodec.jsonCodec(MongoTableHandle.class);

    @Test
    public void testRoundTrip()
    {
        MongoTableHandle expected = new MongoTableHandle(new SchemaTableName("schema", "table"));

        String json = codec.toJson(expected);
        MongoTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
    }
}
