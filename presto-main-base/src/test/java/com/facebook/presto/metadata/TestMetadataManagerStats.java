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
package com.facebook.presto.metadata;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestMetadataManagerStats
{
    @Test
    public void testInitialState()
    {
        MetadataManagerStats stats = new MetadataManagerStats();

        // Verify all counters start at 0
        assertEquals(stats.getListSchemaNamesCalls(), 0);
        assertEquals(stats.getListTablesCalls(), 0);
        assertEquals(stats.getGetTableMetadataCalls(), 0);
        assertEquals(stats.getGetColumnHandlesCalls(), 0);

        // Verify timing stats are initialized
        assertNotNull(stats.getListSchemaNamesTime());
        assertNotNull(stats.getListTablesTime());
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 0.0);
        assertEquals(stats.getListTablesTime().getAllTime().getCount(), 0.0);
    }

    @Test
    public void testRecordCalls()
    {
        MetadataManagerStats stats = new MetadataManagerStats();

        // Record some calls
        stats.recordListSchemaNamesCall(1000000); // 1ms in nanoseconds
        stats.recordListSchemaNamesCall(2000000); // 2ms
        stats.recordListTablesCall(3000000); // 3ms

        // Verify counters incremented
        assertEquals(stats.getListSchemaNamesCalls(), 2);
        assertEquals(stats.getListTablesCalls(), 1);

        // Verify timing recorded
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 2.0);
        assertEquals(stats.getListTablesTime().getAllTime().getCount(), 1.0);
    }

    @Test
    public void testTimingStatistics()
    {
        MetadataManagerStats stats = new MetadataManagerStats();

        // Record calls with different durations
        stats.recordListSchemaNamesCall(1000000); // 1ms
        stats.recordListSchemaNamesCall(5000000); // 5ms
        stats.recordListSchemaNamesCall(3000000); // 3ms

        // Verify count
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 3.0);

        // Verify min/max are reasonable
        assertTrue(stats.getListSchemaNamesTime().getAllTime().getMin() > 0);
        assertTrue(stats.getListSchemaNamesTime().getAllTime().getMax() > 0);
        assertTrue(stats.getListSchemaNamesTime().getAllTime().getMax() >= stats.getListSchemaNamesTime().getAllTime().getMin());
    }

    @Test
    public void testMultipleOperations()
    {
        MetadataManagerStats stats = new MetadataManagerStats();

        // Record various operations
        stats.recordListSchemaNamesCall(1000000);
        stats.recordListTablesCall(2000000);
        stats.recordGetTableMetadataCall(3000000);
        stats.recordGetColumnHandlesCall(4000000);

        // Verify all were recorded independently
        assertEquals(stats.getListSchemaNamesCalls(), 1);
        assertEquals(stats.getListTablesCalls(), 1);
        assertEquals(stats.getGetTableMetadataCalls(), 1);
        assertEquals(stats.getGetColumnHandlesCalls(), 1);

        // Verify timing for each
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 1.0);
        assertEquals(stats.getListTablesTime().getAllTime().getCount(), 1.0);
        assertEquals(stats.getGetTableMetadataTime().getAllTime().getCount(), 1.0);
        assertEquals(stats.getGetColumnHandlesTime().getAllTime().getCount(), 1.0);
    }
}
