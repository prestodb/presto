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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.TestingDatabase.CONNECTOR_ID;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJdbcHandleResolver
{
    private static final JdbcHandleResolver RESOLVER = new JdbcHandleResolver(new JdbcConnectorId(CONNECTOR_ID));

    @Test
    public void testCanHandle()
            throws Exception
    {
        assertTrue(RESOLVER.canHandle(createTableHandle(CONNECTOR_ID)));
        assertFalse(RESOLVER.canHandle(createTableHandle("unknown")));
    }

    @Test
    public void testCanHandleRecordSet()
    {
        assertTrue(RESOLVER.canHandle(createSplit(CONNECTOR_ID)));
        assertFalse(RESOLVER.canHandle(createSplit("unknown")));
    }

    private static JdbcTableHandle createTableHandle(String connectorId)
    {
        return new JdbcTableHandle(connectorId, new SchemaTableName("schema", "table"), "catalog", "schema", "table");
    }

    private static JdbcSplit createSplit(String connectorId)
    {
        return new JdbcSplit(connectorId, "catalog", "schema", "table", "connectionUrl", ImmutableMap.<String, String>of(), TupleDomain.<ColumnHandle>all());
    }
}
