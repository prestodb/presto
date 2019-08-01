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
package com.facebook.presto.pinot;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestPinotTableHandle
{
    private final PinotTableHandle tableHandle = new PinotTableHandle("connectorId", "schemaName", "tableName");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<PinotTableHandle> codec = jsonCodec(PinotTableHandle.class);
        String json = codec.toJson(tableHandle);
        PinotTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new PinotTableHandle("connector", "schema", "table"),
                        new PinotTableHandle("connector", "schema", "table"))
                .addEquivalentGroup(
                        new PinotTableHandle("connectorX", "schema", "table"),
                        new PinotTableHandle("connectorX", "schema", "table"))
                .addEquivalentGroup(
                        new PinotTableHandle("connector", "schemaX", "table"),
                        new PinotTableHandle("connector", "schemaX", "table"))
                .addEquivalentGroup(
                        new PinotTableHandle("connector", "schema", "tableX"),
                        new PinotTableHandle("connector", "schema", "tableX"))
                .check();
    }
}
