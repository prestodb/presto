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
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static org.testng.Assert.assertEquals;

public class TestJdbcSplit
{
    private final JdbcSplit split = new JdbcSplit("connectorId", "catalog", "schemaName", "tableName", TupleDomain.all(), Optional.empty());

    @Test
    public void testAddresses()
    {
        // split uses "example" scheme so no addresses are available and is not remotely accessible
        assertEquals(split.getAddresses(), ImmutableList.of());
        assertEquals(split.getNodeSelectionStrategy(), NO_PREFERENCE);

        JdbcSplit jdbcSplit = new JdbcSplit("connectorId", "catalog", "schemaName", "tableName", TupleDomain.all(), Optional.empty());
        assertEquals(jdbcSplit.getAddresses(), ImmutableList.of());
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<JdbcSplit> codec = jsonCodec(JdbcSplit.class);
        String json = codec.toJson(split);
        JdbcSplit copy = codec.fromJson(json);
        assertEquals(copy.getConnectorId(), split.getConnectorId());
        assertEquals(copy.getSchemaName(), split.getSchemaName());
        assertEquals(copy.getTableName(), split.getTableName());

        assertEquals(copy.getAddresses(), ImmutableList.of());
        assertEquals(copy.getNodeSelectionStrategy(), NO_PREFERENCE);
    }
}
