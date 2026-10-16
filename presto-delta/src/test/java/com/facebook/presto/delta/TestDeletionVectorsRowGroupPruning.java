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
import org.testng.annotations.DataProvider;
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
 *
 * In order to test further data layouts, the following tables are used:
 *    rg_mdata_sdv: 60 data files with shuffled data, 1 unique deletion vector
 *    rg_mdata_sdv_opt: same as rg_mdata_sdv, but letting Databricks optimize the table,
 *                      which changes file layouts and rewrites some data and DVs
 *    rg_mdata_sdv: 1 data file, 1 deletion vector per delete statement
 *    rg_mdata_sdv: 60 data files with shuffled data, 1 deletion vector per delete statement
 * All those tables do contain the same data, so the tests should be the same, thus they are
 * executed iteratively through a DataProvider with 4 configurations
 */
public class TestDeletionVectorsRowGroupPruning
        extends AbstractDeltaDistributedQueryTestBase
{
    @DataProvider(name = "tableNames")
    public Object[][] tableNames()
    {
        return new Object[][] {
                {"row_group_dvs"},
                {"rg_mdata_sdv"},
                {"rg_mdata_sdv_opt"},
                {"rg_sdata_mdv"},
                {"rg_mdata_mdv"}
        };
    }

    @Test(dataProvider = "tableNames")
    public void testRowGroupPruningWithRG0AndRG2Pruned(String tableName)
    {
        // Filter that prunes RG0 and RG2, reads only RG1
        // WHERE id BETWEEN 110 AND 175
        // RG1 originally has 100 rows (id 101-200), but we deleted id=105, 120, 150, 175
        // id 110-175 range originally has 66 rows, minus deleted (120, 150, 175) = 63
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id BETWEEN 110 AND 175",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));

        assertQuery(session, query, "SELECT 63");
    }

    @Test(dataProvider = "tableNames")
    public void testRowGroupPruningWithRG0Pruned(String tableName)
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
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));

        assertQuery(session, query, "SELECT 193");
    }

    @Test(dataProvider = "tableNames")
    public void testRowGroupPruningWithRG1AndRG2Pruned(String tableName)
    {
        // Filter that prunes RG1 and RG2, reads only RG0
        // WHERE id <= 100
        // RG0: 100 - 3 deleted (10,50,90) = 97
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id <= 100",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));

        assertQuery(session, query, "SELECT 97");
    }

    @Test(dataProvider = "tableNames")
    public void testFullScanWithAllDVsApplied(String tableName)
    {
        // No pruning (full scan)
        // Total: 300 - 10 deleted = 290
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));

        assertQuery(session, query, "SELECT 290");
    }

    @Test(dataProvider = "tableNames")
    public void testAllDeletedRowsAreAbsent(String tableName)
    {
        // Verify all deleted rows are absent
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Deleted IDs from all row groups: 10, 50, 90, 105, 120, 150, 175, 210, 250, 290
        for (int id : new int[]{10, 50, 90, 105, 120, 150, 175, 210, 250, 290}) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id = %d",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName), id);
            assertQuery(session, query, "SELECT 0");
        }
    }

    @Test(dataProvider = "tableNames")
    public void testAdjacentNonDeletedRowsArePresent(String tableName)
    {
        // Verify specific non-deleted rows adjacent to deleted ones ARE present
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Adjacent IDs to deleted rows: 9, 11, 49, 51, 89, 91, 104, 106, 119, 121, 149, 151, 174, 176, 209, 211, 249, 251, 289, 291
        int[] adjacentIds = {9, 11, 49, 51, 89, 91, 104, 106, 119, 121, 149, 151, 174, 176, 209, 211, 249, 251, 289, 291};

        for (int id : adjacentIds) {
            String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id = %d",
                    PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName), id);
            assertQuery(session, query, "SELECT 1");
        }
    }

    @Test(dataProvider = "tableNames")
    public void testCriticalRow106AfterDeleted105(String tableName)
    {
        // Test 7: Critical test — filter on RG1 range that would break with a wrong DV offset
        // WHERE id = 106 (RG1, just after deleted id=105)
        // If DV absolute position 104 is misinterpreted as relative, a wrong row gets deleted
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT id, value FROM \"%s\".\"%s\" WHERE id = 106 ORDER BY id ASC",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));

        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 1, "Should return 1 row for id=106");
        assertEquals(result.getMaterializedRows().get(0).getField(0), 106);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 1060);
    }

    @Test(dataProvider = "tableNames")
    public void testRowGroupPruningInInClauses(String tableName)
    {
        // Verify all deleted rows are absent
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Deleted IDs from all row groups: 10, 50, 90, 105, 120, 150, 175, 210, 250, 290
        String query = format("SELECT count(*) FROM \"%s\".\"%s\" WHERE id IN (10, 50, 90, 105, 120, 150, 175, 210, 250, 290)",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));
        assertQuery(session, query, "SELECT 0");
    }

    @Test(dataProvider = "tableNames")
    public void testRowGroupPruningInInClausesAdjacent(String tableName)
    {
        // Verify all deleted rows are absent
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Deleted IDs from all row groups: 10, 120, 290
        String query = format("SELECT * FROM \"%s\".\"%s\" WHERE id IN (10, 11, 120, 121, 289, 290) ORDER BY id ASC",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 3, "Should return 3 row for id=11, id=121, id=289");
        assertEquals(result.getMaterializedRows().get(0).getField(0), 11);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 121);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 289);
    }

    @Test(dataProvider = "tableNames")
    public void testRowGroupPruningWithDeletionVectorsDisabled(String tableName)
    {
        // When deletion vectors are disabled, all rows including deleted ones should be returned
        // Expected: 300 rows (no deletions applied)
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "false")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, tableName));

        // rg_mdata_sdv_opt corresponds to a table that has been automatically optimized by databricks after
        // executing the DELETE statement, so that parquet files were rewritten with the deleted rows directly.
        // We add this case to also test consistency on delta tables following the "default" databricks
        // optimization lifecycle
        assertQuery(session, query, "rg_mdata_sdv_opt".equals(tableName) ? "SELECT 290" : "SELECT 300");
    }
}
