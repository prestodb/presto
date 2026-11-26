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
package com.facebook.presto.delta;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import org.testng.annotations.Test;

import static com.facebook.presto.delta.DeltaSessionProperties.DELETION_VECTORS_ENABLED;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Integration tests for reading Delta tables with deletion vectors and partition pruning.
 * Tests verify that deletion vectors are correctly applied when partition pruning is active.
 *
 * Table: prune_dvs
 * Partitions: A, B, C, D (column: part_col)
 * Files per partition: 3 (one per row_group_id: 1, 2, 3)
 * Total files: 12
 * Deletion vectors: present in ALL files across ALL partitions
 *
 * Deleted IDs (from all partitions):
 *   - row_group_id=1: ids 5, 15, 25, 50
 *   - row_group_id=2: ids 110, 150, 175
 *   - row_group_id=3: ids 210, 250, 290
 *
 * Expected counts:
 *   - Total rows: 1160
 *   - Per partition: 290
 *   - Per partition per row_group_id=1: 96 (100 - 4 deleted)
 *   - Per partition per row_group_id=2: 97 (100 - 3 deleted)
 *   - Per partition per row_group_id=3: 97 (100 - 3 deleted)
 */
public class TestDeletionVectorsWithPartitionPruning
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void testSinglePartitionFilterWithCount()
    {
        // Single partition filter (partition pruning) — verify DVs are respected
        // Query: SELECT COUNT(*) FROM prune_dvs WHERE part_col = 'A'
        // Expected: 290 (300 total - 10 deleted across all row groups)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" where part_col = 'A'",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"));

        assertQuery(session, query, "SELECT 290");
    }

    @Test
    public void testPartitionAndRowGroupFilter()
    {
        // Partition filter + row_group filter (prunes earlier row groups)
        // Query: SELECT COUNT(*) FROM prune_dvs WHERE part_col = 'B' AND row_group_id = 2
        // Expected: 97 (100 rows - 3 deleted: 110, 150, 175)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE part_col = 'B' AND row_group_id = 2",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"));

        assertQuery(session, query, "SELECT 97");
    }

    @Test
    public void testDeletedIdReturnsZero()
    {
        // Specific IDs that were deleted — should return 0
        // Query: SELECT * FROM prune_dvs WHERE part_col = 'C' AND id = 50
        // Expected: 0 (row was deleted)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE part_col = 'C' AND id = 50",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"));

        assertQuery(session, query, "SELECT 0");
    }

    @Test
    public void testNonDeletedIdReturnsOne()
    {
        // Specific IDs that were NOT deleted — should return 1
        // Query: SELECT * FROM prune_dvs WHERE part_col = 'C' AND id = 51
        // Expected: 1 (row exists)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE part_col = 'C' AND id = 51",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"));

        assertQuery(session, query, "SELECT 1");
    }

    @Test
    public void testOrderedDataFromPartition()
    {
        // All data from one partition, ordered
        // Query: SELECT id, row_group_id, value FROM prune_dvs
        //        WHERE part_col = 'D' AND row_group_id = 1
        //        ORDER BY id LIMIT 10
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT id, row_group_id FROM \"%s\".\"%s\" WHERE part_col = 'D' AND row_group_id = 1 ORDER BY id LIMIT 10",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"));

        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 10, "Should return 10 rows");

        // Verify ordering and that deleted IDs (5, 15, 25, 50) are not present
        // First 10 non-deleted IDs should be: 1, 2, 3, 4, 6, 7, 8, 9, 10, 11
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(3).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(4).getField(0), 6);  // 5 is deleted
        assertEquals(result.getMaterializedRows().get(5).getField(0), 7);
        assertEquals(result.getMaterializedRows().get(6).getField(0), 8);
        assertEquals(result.getMaterializedRows().get(7).getField(0), 9);
        assertEquals(result.getMaterializedRows().get(8).getField(0), 10);
        assertEquals(result.getMaterializedRows().get(9).getField(0), 11);
    }

    @Test
    public void testTotalRowCountAcrossAllPartitions()
    {
        // Verify total count across all partitions
        // 1160 rows (1200 total - 40 deleted across all partitions)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"));

        assertQuery(session, query, "SELECT 1160");
    }

    @Test
    public void testRowGroupId1CountPerPartition()
    {
        // Verify count for row_group_id=1 in each partition
        // 96 per partition (100 - 4 deleted: 5, 15, 25, 50)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        for (String partition : new String[]{"A", "B", "C", "D"}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE part_col = '%s' AND row_group_id = 1",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"), partition);

            assertQuery(session, query, "SELECT 96");
        }
    }

    @Test
    public void testRowGroupId2CountPerPartition()
    {
        // Verify count for row_group_id=2 in each partition
        // 97 per partition (100 - 3 deleted: 110, 150, 175)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        for (String partition : new String[]{"A", "B", "C", "D"}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE part_col = '%s' AND row_group_id = 2",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"), partition);

            assertQuery(session, query, "SELECT 97");
        }
    }

    @Test
    public void testRowGroupId3CountPerPartition()
    {
        // Verify count for row_group_id=3 in each partition
        // 97 per partition (100 - 3 deleted: 210, 250, 290)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        for (String partition : new String[]{"A", "B", "C", "D"}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE part_col = '%s' AND row_group_id = 3",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"), partition);

            assertQuery(session, query, "SELECT 97");
        }
    }

    @Test
    public void testAllDeletedIdsReturnZero()
    {
        // Verify all deleted IDs return 0 rows across all partitions
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // row_group_id=1 deleted IDs: 5, 15, 25, 50
        for (int id : new int[]{5, 15, 25, 50}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id = %d",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"), id);
            assertQuery(session, query, "SELECT 0");
        }

        // row_group_id=2 deleted IDs: 110, 150, 175
        for (int id : new int[]{110, 150, 175}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id = %d",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"), id);
            assertQuery(session, query, "SELECT 0");
        }

        // row_group_id=3 deleted IDs: 210, 250, 290
        for (int id : new int[]{210, 250, 290}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id = %d",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"), id);
            assertQuery(session, query, "SELECT 0");
        }
    }

    @Test
    public void testPartitionPruningWithDeletionVectorsDisabled()
    {
        // When deletion vectors are disabled, all rows including deleted ones should be returned
        // 300 rows per partition (no deletions applied)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "false")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE part_col = 'A'",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "prune_dvs"));

        assertQuery(session, query, "SELECT 300");
    }
}
