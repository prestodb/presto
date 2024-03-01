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
package com.facebook.presto.druid;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestDruidTableHandle
{
    private final DruidTableHandle tableHandle = new DruidTableHandle("schemaName", "tableName", Optional.empty());

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<DruidTableHandle> codec = jsonCodec(DruidTableHandle.class);
        String json = codec.toJson(tableHandle);
        DruidTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new DruidTableHandle("schema", "table", Optional.empty()),
                        new DruidTableHandle("schema", "table", Optional.empty()))
                .addEquivalentGroup(
                        new DruidTableHandle("schemaX", "table", Optional.empty()),
                        new DruidTableHandle("schemaX", "table", Optional.empty()))
                .addEquivalentGroup(
                        new DruidTableHandle("schema", "tableX", Optional.empty()),
                        new DruidTableHandle("schema", "tableX", Optional.empty()))
                .check();
    }
}
