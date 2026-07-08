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
 * Integration tests for reading Delta tables with deletion vectors and row group pruning.
 * Tests verify that deletion vectors are correctly applied when row group pruning is active.
 *
 * Table: row_group_dvs
 * Schema: id INT, value INT, label STRING
 * Files: 1 parquet file with 3 row groups
 * Deletion vectors: 1 deletion vector (absolute positions in single file)
 *
 * Row Group Layout:
 *   RG0: 100 rows, id 1-100   (absolute positions 0-99)
 *   RG1: 100 rows, id 101-200 (absolute positions 100-199)
 *   RG2: 100 rows, id 201-300 (absolute positions 200-299)
 *
 * Deleted rows (10 total, DV uses absolute file positions):
 *   From RG0: id=10 (pos 9), id=50 (pos 49), id=90 (pos 89)
 *   From RG1: id=105 (pos 104), id=120 (pos 119), id=150 (pos 149), id=175 (pos 174)
 *   From RG2: id=210 (pos 209), id=250 (pos 249), id=290 (pos 289)
 *
 * When filter 'id BETWEEN 110 AND 175' prunes RG0, if the reader misinterprets DV absolute
 * position 104 (id=105) as relative to the first non-pruned row group, it would incorrectly
 * delete row at position 4 of RG1 (id=105), or delete the wrong row entirely
 * (e.g., id=104+offset). The tests verify that:
 * 1. Deleted rows (105, 120, 150, 175) are correctly excluded
 * 2. Adjacent rows (106, 121, 151, 176) are correctly included
 */
public class TestDeletionVectorsRowGroupPruning
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void testRowGroupPruningWithRG0AndRG2Pruned()
    {
        // Filter that prunes RG0 and RG2, reads only RG1
        // WHERE id BETWEEN 110 AND 175
        // RG1 originally has 100 rows (id 101-200), but we deleted id=105, 120, 150, 175
        // id 110-175 range originally has 66 rows, minus deleted (120, 150, 175) = 63
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id BETWEEN 110 AND 175",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));

        assertQuery(session, query, "SELECT 63");
    }

    @Test
    public void testRowGroupPruningWithRG0Pruned()
    {
        // Filter that prunes RG0 only, reads RG1 and RG2
        // WHERE id > 100
        // RG1: 100 - 4 deleted (105,120,150,175) = 96
        // RG2: 100 - 3 deleted (210,250,290) = 97
        // Total: 193
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id > 100",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));

        assertQuery(session, query, "SELECT 193");
    }

    @Test
    public void testRowGroupPruningWithRG1AndRG2Pruned()
    {
        // Filter that prunes RG1 and RG2, reads only RG0
        // WHERE id <= 100
        // RG0: 100 - 3 deleted (10,50,90) = 97
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id <= 100",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));

        assertQuery(session, query, "SELECT 97");
    }

    @Test
    public void testFullScanWithAllDVsApplied()
    {
        // No pruning (full scan)
        // Total: 300 - 10 deleted = 290
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));

        assertQuery(session, query, "SELECT 290");
    }

    @Test
    public void testAllDeletedRowsAreAbsent()
    {
        // Verify all deleted rows are absent
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Deleted IDs from all row groups: 10, 50, 90, 105, 120, 150, 175, 210, 250, 290
        for (int id : new int[]{10, 50, 90, 105, 120, 150, 175, 210, 250, 290}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id = %d",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"), id);
            assertQuery(session, query, "SELECT 0");
        }
    }

    @Test
    public void testAdjacentNonDeletedRowsArePresent()
    {
        // Verify specific non-deleted rows adjacent to deleted ones ARE present
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Adjacent IDs to deleted rows: 9, 11, 49, 51, 89, 91, 104, 106, 119, 121, 149, 151, 174, 176, 209, 211, 249, 251, 289, 291
        int[] adjacentIds = {9, 11, 49, 51, 89, 91, 104, 106, 119, 121, 149, 151, 174, 176, 209, 211, 249, 251, 289, 291};

        for (int id : adjacentIds) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id = %d",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"), id);
            assertQuery(session, query, "SELECT 1");
        }
    }

    @Test
    public void testCriticalRow106AfterDeleted105()
    {
        // Test 7: Critical test — filter on RG1 range that would break with a wrong DV offset
        // WHERE id = 106 (RG1, just after deleted id=105)
        // If DV absolute position 104 is misinterpreted as relative, a wrong row gets deleted
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT id, value FROM \"%s\".\"%s\" WHERE id = 106",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));

        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 1, "Should return 1 row for id=106");
        assertEquals(result.getMaterializedRows().get(0).getField(0), 106);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 1060);
    }

    @Test
    public void testRowGroupPruningInInClauses()
    {
        // Verify all deleted rows are absent
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Deleted IDs from all row groups: 10, 50, 90, 105, 120, 150, 175, 210, 250, 290
        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id IN (10, 50, 90, 105, 120, 150, 175, 210, 250, 290)",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));
        assertQuery(session, query, "SELECT 0");
    }

    @Test
    public void testRowGroupPruningInInClausesAdjacent()
    {
        // Verify all deleted rows are absent
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Deleted IDs from all row groups: 10, 120, 290
        String query = format("SELECT * FROM \"%s\".\"%s\" WHERE id IN (10, 11, 120, 121, 289, 290)",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 3, "Should return 3 row for id=11, id=121, id=289");
        assertEquals(result.getMaterializedRows().get(0).getField(0), 11);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 121);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 289);
    }

    @Test
    public void testRowGroupPruningWithDeletionVectorsDisabled()
    {
        // When deletion vectors are disabled, all rows including deleted ones should be returned
        // Expected: 300 rows (no deletions applied)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "false")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "row_group_dvs"));

        assertQuery(session, query, "SELECT 300");
    }
}
