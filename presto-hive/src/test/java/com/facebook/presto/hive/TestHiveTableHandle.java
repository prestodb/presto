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

import com.facebook.presto.spi.Session;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static org.testng.Assert.assertEquals;

public class TestHiveTableHandle
{
    private static final Session SESSION = new Session("user", "test", "catalog", "test", UTC_KEY, Locale.ENGLISH, null, null);
    private final JsonCodec<HiveTableHandle> codec = JsonCodec.jsonCodec(HiveTableHandle.class);

    @Test
    public void testRoundTrip()
    {
        HiveTableHandle expected = new HiveTableHandle("client", "schema", "table", SESSION);

        String json = codec.toJson(expected);
        HiveTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getClientId(), expected.getClientId());
        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
    }
}
