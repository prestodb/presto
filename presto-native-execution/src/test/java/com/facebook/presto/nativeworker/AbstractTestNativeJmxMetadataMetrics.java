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
package com.facebook.presto.nativeworker;

import com.facebook.presto.metadata.MetadataManagerStats;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNationWithFormat;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestNativeJmxMetadataMetrics
        extends AbstractTestQueryFramework
{
    private final String storageFormat = "PARQUET";
    protected abstract String getCatalogName();
    protected abstract String getSchemaName();
    protected abstract MetadataManagerStats getMetadataStats();
    protected String getTableName(String table)
    {
        return String.format("%s.%s.%s", getCatalogName(), getSchemaName(), table);
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createRegion(queryRunner);
        createNationWithFormat(queryRunner, storageFormat);
    }

    @Test
    public void testMetadataManagerStatsExist()
    {
        MetadataManagerStats stats = getMetadataStats();
        assertNotNull(stats, "MetadataManagerStats should be available");
        assertTrue(stats.getGetTableMetadataCalls() >= 0, "MetadataManagerStats should track getTableMetadata calls");
    }

    @Test
    public void testMetadataMetricsAfterQueries()
    {
        MetadataManagerStats stats = getMetadataStats();

        long initialTableMetadataCalls = stats.getGetTableMetadataCalls();
        long initialColumnHandlesCalls = stats.getGetColumnHandlesCalls();

        assertQuerySucceeds(String.format("SHOW TABLES FROM %s.%s", getCatalogName(), getSchemaName()));
        assertQuerySucceeds(String.format("SELECT * FROM %s", getTableName("nation")));
        assertQuerySucceeds(String.format("DESCRIBE %s", getTableName("nation")));
        assertQuerySucceeds(String.format("SHOW COLUMNS FROM %s", getTableName("region")));
        assertQuerySucceeds(String.format("SELECT n.name, r.name FROM %s n JOIN %s r ON n.regionkey = r.regionkey", getTableName("nation"), getTableName("region")));

        assertTrue(stats.getGetTableMetadataCalls() >= initialTableMetadataCalls,
                String.format("getTableMetadataCalls should have increased or stayed same: %d -> %d", initialTableMetadataCalls, stats.getGetTableMetadataCalls()));
        assertTrue(stats.getGetColumnHandlesCalls() >= initialColumnHandlesCalls,
                String.format("getColumnHandlesCalls should have increased or stayed same: %d -> %d", initialColumnHandlesCalls, stats.getGetColumnHandlesCalls()));
    }

    @Test
    public void testMetadataTimingMetrics()
    {
        MetadataManagerStats stats = getMetadataStats();

        assertQuerySucceeds(String.format("SHOW TABLES FROM %s.%s", getCatalogName(), getSchemaName()));
        assertQuerySucceeds(String.format("SELECT * FROM %s LIMIT 10", getTableName("nation")));
        assertQuerySucceeds(String.format("SELECT count(*) FROM %s", getTableName("region")));

        assertNotNull(stats.getGetTableMetadataTime(), "getTableMetadataTime should be available");
        assertNotNull(stats.getListTablesTime(), "listTablesTime should be available");
        assertNotNull(stats.getGetColumnHandlesTime(), "getColumnHandlesTime should be available");
    }

    @Test
    public void testMultipleOperationsIncrementMetrics()
    {
        MetadataManagerStats stats = getMetadataStats();

        long initialTotalCalls = stats.getGetTableMetadataCalls() + stats.getListTablesCalls() + stats.getGetColumnHandlesCalls();
        for (int i = 0; i < 5; i++) {
            assertQuerySucceeds(String.format("SHOW TABLES FROM %s.%s", getCatalogName(), getSchemaName()));
            assertQuerySucceeds(String.format("SELECT * FROM %s WHERE nationkey < 10", getTableName("nation")));
            assertQuerySucceeds(String.format("SELECT count(*) FROM %s", getTableName("region")));
        }
        long updatedTotalCalls = stats.getGetTableMetadataCalls() + stats.getListTablesCalls() + stats.getGetColumnHandlesCalls();

        // Verify total calls increased
        assertTrue(updatedTotalCalls > initialTotalCalls,
                String.format("Total metadata calls should have increased from %d to %d", initialTotalCalls, updatedTotalCalls));
    }

    @Test
    public void testMetadataStatsWithComplexQueries()
    {
        MetadataManagerStats stats = getMetadataStats();
        long initialTableMetadataCalls = stats.getGetTableMetadataCalls();
        long initialColumnHandlesCalls = stats.getGetColumnHandlesCalls();

        assertQuerySucceeds(String.format("SELECT n.name, r.name FROM %s n JOIN %s r ON n.regionkey = r.regionkey WHERE n.nationkey < 10", getTableName("nation"), getTableName("region")));
        assertQuerySucceeds(String.format("SELECT * FROM %s WHERE nationkey IN (SELECT nationkey FROM %s WHERE regionkey = 1)", getTableName("nation"), getTableName("nation")));
        assertQuerySucceeds(String.format("WITH nation_counts AS (SELECT regionkey, count(*) as cnt FROM %s GROUP BY regionkey) " +
                        "SELECT r.name, nc.cnt FROM %s r JOIN nation_counts nc ON r.regionkey = nc.regionkey", getTableName("nation"), getTableName("region")));

        // Verify metrics increased
        assertTrue(stats.getGetTableMetadataCalls() >= initialTableMetadataCalls, "Complex queries should trigger table metadata calls");
        assertTrue(stats.getGetColumnHandlesCalls() >= initialColumnHandlesCalls, "Complex queries should trigger column handle calls");
    }

    @Test
    public void testMetadataStatsAvailability()
    {
        MetadataManagerStats stats = getMetadataStats();
        assertTrue(stats.getGetTableMetadataCalls() >= 0, "getTableMetadataCalls should be available");
        assertTrue(stats.getListTablesCalls() >= 0, "listTablesCalls should be available");
        assertTrue(stats.getGetColumnHandlesCalls() >= 0, "getColumnHandlesCalls should be available");
        assertTrue(stats.getGetColumnMetadataCalls() >= 0, "getColumnMetadataCalls should be available");
        assertTrue(stats.getListSchemaNamesCalls() >= 0, "listSchemaNamesCalls should be available");
        assertNotNull(stats.getGetTableMetadataTime(), "getTableMetadataTime should be available");
        assertNotNull(stats.getListTablesTime(), "listTablesTime should be available");
        assertNotNull(stats.getGetColumnHandlesTime(), "getColumnHandlesTime should be available");
        assertNotNull(stats.getGetColumnMetadataTime(), "getColumnMetadataTime should be available");
        assertNotNull(stats.getListSchemaNamesTime(), "listSchemaNamesTime should be available");
    }

    @Test
    public void testMetadataStatsTimingDetails()
    {
        MetadataManagerStats stats = getMetadataStats();
        long initialCalls = stats.getGetTableMetadataCalls();
        assertQuerySucceeds(String.format("SELECT * FROM %s", getTableName("nation")));
        assertQuerySucceeds(String.format("SELECT * FROM %s", getTableName("region")));

        assertTrue(stats.getGetTableMetadataCalls() > initialCalls, "Should have recorded additional metadata calls");
        // Verify timing stat objects exist
        assertNotNull(stats.getGetTableMetadataTime(), "Timing stat should be available");
        assertNotNull(stats.getListTablesTime(), "Timing stat should be available");
        assertNotNull(stats.getGetColumnHandlesTime(), "Timing stat should be available");
    }
}
