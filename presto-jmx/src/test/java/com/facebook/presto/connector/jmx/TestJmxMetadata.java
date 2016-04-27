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
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJmxMetadata
{
    private static final String RUNTIME_OBJECT = "java.lang:type=Runtime";
    private static final SchemaTableName RUNTIME_TABLE = new SchemaTableName("jmx", RUNTIME_OBJECT.toLowerCase(ENGLISH));

    private final JmxMetadata metadata = new JmxMetadata("test", getPlatformMBeanServer());

    @Test
    public void testListSchemas()
            throws Exception
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("jmx"));
    }

    @Test
    public void testListTables()
    {
        assertTrue(metadata.listTables(SESSION, "jmx").contains(RUNTIME_TABLE));
    }

    @Test
    public void testGetTableHandle()
            throws Exception
    {
        JmxTableHandle handle = metadata.getTableHandle(SESSION, RUNTIME_TABLE);
        assertEquals(handle.getConnectorId(), "test");
        assertEquals(handle.getObjectName(), RUNTIME_OBJECT);

        List<JmxColumnHandle> columns = handle.getColumns();
        assertTrue(columns.contains(new JmxColumnHandle("test", "node", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("test", "Name", createUnboundedVarcharType())));
        assertTrue(columns.contains(new JmxColumnHandle("test", "StartTime", BigintType.BIGINT)));
    }
}
