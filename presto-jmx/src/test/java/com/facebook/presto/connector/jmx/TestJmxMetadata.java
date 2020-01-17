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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.connector.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static com.facebook.presto.connector.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJmxMetadata
{
    private static final String RUNTIME_OBJECT = "java.lang:type=Runtime";
    private static final SchemaTableName RUNTIME_TABLE = new SchemaTableName(JMX_SCHEMA_NAME, RUNTIME_OBJECT.toLowerCase(ENGLISH));
    private static final SchemaTableName RUNTIME_HISTORY_TABLE = new SchemaTableName(HISTORY_SCHEMA_NAME, RUNTIME_OBJECT.toLowerCase(ENGLISH));

    private final JmxMetadata metadata = new JmxMetadata(getPlatformMBeanServer(), new JmxHistoricalData(1000, ImmutableSet.of(RUNTIME_OBJECT.toLowerCase(ENGLISH))));

    @Test
    public void testListSchemas()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of(JMX_SCHEMA_NAME, HISTORY_SCHEMA_NAME));
    }

    @Test
    public void testListTables()
    {
        assertTrue(metadata.listTables(SESSION, JMX_SCHEMA_NAME).contains(RUNTIME_TABLE));
        assertTrue(metadata.listTables(SESSION, HISTORY_SCHEMA_NAME).contains(RUNTIME_HISTORY_TABLE));
    }

    @Test
    public void testGetTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, RUNTIME_TABLE);
        assertEquals(handle.getObjectNames(), ImmutableList.of(RUNTIME_OBJECT));

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertTrue(columns.contains(new JmxColumnHandle("node", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("Name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("StartTime", BIGINT)));
    }

    @Test
    public void testGetTimeTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, RUNTIME_HISTORY_TABLE);
        assertEquals(handle.getObjectNames(), ImmutableList.of(RUNTIME_OBJECT));

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertTrue(columns.contains(new JmxColumnHandle("timestamp", TIMESTAMP)));
        assertTrue(columns.contains(new JmxColumnHandle("node", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("Name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("StartTime", BIGINT)));
    }

    @Test
    public void testGetCumulativeTableHandle()
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*"));
        assertTrue(handle.getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(handle.getObjectNames().size() > 1);

        List<JmxColumnHandle> columns = handle.getColumnHandles();
        assertTrue(columns.contains(new JmxColumnHandle("node", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("object_name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("Name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("StartTime", BIGINT)));

        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*java.lang:type=Runtime*")).getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "java.lang:*=Runtime")).getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*")).getObjectNames().contains(RUNTIME_OBJECT));
        assertTrue(metadata.getTableHandle(SESSION, new SchemaTableName(JMX_SCHEMA_NAME, "*:*")).getObjectNames().contains(RUNTIME_OBJECT));
    }
}
