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
package com.facebook.presto.example;

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestExampleColumnHandle
{
    private final ExampleColumnHandle columnHandle = new ExampleColumnHandle("connectorId", "columnName", STRING, 0);

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<ExampleColumnHandle> codec = jsonCodec(ExampleColumnHandle.class);
        String json = codec.toJson(columnHandle);
        ExampleColumnHandle copy = codec.fromJson(json);
        assertEquals(copy, columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new ExampleColumnHandle("connectorId", "columnName", STRING, 0),
                        new ExampleColumnHandle("connectorId", "columnName", STRING, 0),
                        new ExampleColumnHandle("connectorId", "columnName", LONG, 0),
                        new ExampleColumnHandle("connectorId", "columnName", STRING, 1))
                .addEquivalentGroup(
                        new ExampleColumnHandle("connectorIdX", "columnName", STRING, 0),
                        new ExampleColumnHandle("connectorIdX", "columnName", STRING, 0),
                        new ExampleColumnHandle("connectorIdX", "columnName", LONG, 0),
                        new ExampleColumnHandle("connectorIdX", "columnName", STRING, 1))
                .addEquivalentGroup(
                        new ExampleColumnHandle("connectorId", "columnNameX", STRING, 0),
                        new ExampleColumnHandle("connectorId", "columnNameX", STRING, 0),
                        new ExampleColumnHandle("connectorId", "columnNameX", LONG, 0),
                        new ExampleColumnHandle("connectorId", "columnNameX", STRING, 1))
                .check();
    }
}
