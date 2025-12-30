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

import static com.facebook.presto.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStatsRecordingMetadataManager
{
    @Test
    public void testEverythingDelegated()
    {
        assertAllMethodsOverridden(Metadata.class, StatsRecordingMetadataManager.class);
    }

    @Test
    public void testStatsRecording()
    {
        MetadataManagerStats stats = new MetadataManagerStats();

        // Verify initial state - no calls recorded
        assertEquals(stats.getListSchemaNamesCalls(), 0);
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 0.0);

        // Record a call directly (without session to avoid transaction requirement)
        stats.recordListSchemaNamesCall(1000000);

        // Verify stats were recorded
        assertEquals(stats.getListSchemaNamesCalls(), 1);
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 1.0);
    }

    @Test
    public void testMultipleCallsRecorded()
    {
        MetadataManagerStats stats = new MetadataManagerStats();

        // Verify initial state
        assertEquals(stats.getListSchemaNamesCalls(), 0);
        assertEquals(stats.getListTablesCalls(), 0);

        // Record different metadata operations directly
        stats.recordListSchemaNamesCall(1000000);
        stats.recordListSchemaNamesCall(2000000);
        stats.recordListTablesCall(3000000);

        // Verify all calls were recorded
        assertEquals(stats.getListSchemaNamesCalls(), 2);
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 2.0);
        assertEquals(stats.getListTablesCalls(), 1);
        assertEquals(stats.getListTablesTime().getAllTime().getCount(), 1.0);
    }

    @Test
    public void testTimingRecorded()
    {
        MetadataManagerStats stats = new MetadataManagerStats();

        // Record operation with timing
        stats.recordListSchemaNamesCall(5000000);

        // Verify timing was recorded - count should be 1
        assertEquals(stats.getListSchemaNamesTime().getAllTime().getCount(), 1.0);
        // Verify max and min are greater than 0 (some time was recorded)
        assertTrue(stats.getListSchemaNamesTime().getAllTime().getMax() > 0, "Max time should be greater than 0");
        assertTrue(stats.getListSchemaNamesTime().getAllTime().getMin() > 0, "Min time should be greater than 0");
    }
}
