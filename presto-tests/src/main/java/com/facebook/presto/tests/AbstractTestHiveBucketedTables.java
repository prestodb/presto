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
package com.facebook.presto.tests;

import com.facebook.presto.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public abstract class AbstractTestHiveBucketedTables
        extends AbstractTestQueryFramework
{
    protected abstract String getTableName();

    protected abstract String getSourceTableName();

    /**
     * Override this method in subclasses to create necessary tables.
     * Default implementation does nothing.
     */
    protected void createTables()
    {
        // Default: no-op
    }

    /**
     * Test inserting into bucketed unpartitioned tables.
     */
    @Test
    public void testInsertIntoBucketedTables()
    {
        createTables();
        String tableName = getTableName();
        String sourceTable = getSourceTableName();

        try {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
            // Create the bucketed table
            @Language("SQL") String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "    n_nationkey BIGINT,\n" +
                    "    n_name VARCHAR,\n" +
                    "    n_regionkey BIGINT,\n" +
                    "    n_comment VARCHAR\n" +
                    ")\n" +
                    "WITH (\n" +
                    "    format = 'PARQUET',\n" +
                    "    bucketed_by = ARRAY['n_regionkey'],\n" +
                    "    bucket_count = 2\n" +
                    ")";
            getQueryRunner().execute(createTableSql);

            // Insert data twice
            getQueryRunner().execute("INSERT INTO " + tableName + " SELECT * FROM " + sourceTable);
            getQueryRunner().execute("INSERT INTO " + tableName + " SELECT * FROM " + sourceTable);
            // Validate total row count
            MaterializedResult totalCountResult = getQueryRunner().execute(getQueryRunner().getDefaultSession(), "SELECT count(*) FROM " + tableName);
            assertEquals(totalCountResult.getRowCount(), 1, "Expected single row result");
            assertEquals(((Long) totalCountResult.getMaterializedRows().get(0).getField(0)).longValue(), 50L, "Expected 50 total rows");
            // Validate filtered row count
            MaterializedResult filteredCountResult = getQueryRunner().execute(getQueryRunner().getDefaultSession(), "SELECT count(*) FROM " + tableName + " WHERE n_regionkey = 0");
            assertEquals(filteredCountResult.getRowCount(), 1, "Expected single row result");
            assertEquals(((Long) filteredCountResult.getMaterializedRows().get(0).getField(0)).longValue(), 10L, "Expected 10 rows with n_regionkey = 0");
        }
        finally {
            // Ensure cleanup even if the test fails
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
