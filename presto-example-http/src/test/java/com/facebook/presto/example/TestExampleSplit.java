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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static org.testng.Assert.assertEquals;

public class TestExampleSplit
{
    private final ExampleSplit split = new ExampleSplit("connectorId", "schemaName", "tableName", URI.create("http://127.0.0.1/test.file"));

    @Test
    public void testAddresses()
    {
        // http split with default port
        ExampleSplit httpSplit = new ExampleSplit("connectorId", "schemaName", "tableName", URI.create("http://example.com/example"));
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("example.com")));
        assertEquals(httpSplit.getNodeSelectionStrategy(), NO_PREFERENCE);

        // http split with custom port
        httpSplit = new ExampleSplit("connectorId", "schemaName", "tableName", URI.create("http://example.com:8080/example"));
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("example.com", 8080)));
        assertEquals(httpSplit.getNodeSelectionStrategy(), NO_PREFERENCE);

        // http split with default port
        ExampleSplit httpsSplit = new ExampleSplit("connectorId", "schemaName", "tableName", URI.create("https://example.com/example"));
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("example.com")));
        assertEquals(httpsSplit.getNodeSelectionStrategy(), NO_PREFERENCE);

        // http split with custom port
        httpsSplit = new ExampleSplit("connectorId", "schemaName", "tableName", URI.create("https://example.com:8443/example"));
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("example.com", 8443)));
        assertEquals(httpsSplit.getNodeSelectionStrategy(), NO_PREFERENCE);
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<ExampleSplit> codec = jsonCodec(ExampleSplit.class);
        String json = codec.toJson(split);
        ExampleSplit copy = codec.fromJson(json);
        assertEquals(copy.getConnectorId(), split.getConnectorId());
        assertEquals(copy.getSchemaName(), split.getSchemaName());
        assertEquals(copy.getTableName(), split.getTableName());
        assertEquals(copy.getUri(), split.getUri());

        assertEquals(copy.getAddresses(), ImmutableList.of(HostAddress.fromString("127.0.0.1")));
        assertEquals(copy.getNodeSelectionStrategy(), NO_PREFERENCE);
    }
}
