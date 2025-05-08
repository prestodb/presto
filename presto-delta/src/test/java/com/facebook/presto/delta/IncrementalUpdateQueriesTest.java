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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class IncrementalUpdateQueriesTest
        extends AbstractDeltaDistributedQueryTestBase
{
    private final String version = "delta_v3";
    private final String controlTableName = "deltatbl-partition-prune";
    private final String targetTableName = controlTableName + "-incremental";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = super.createQueryRunner();
        Path path = Paths.get(version);
        registerDeltaTableInHMS(queryRunner,
                path.resolve(targetTableName).toString(),
                path.resolve(targetTableName).toString());
        return queryRunner;
    }

    private void checkQueryOutputOnIncrementalWithNullRows(String controlTableQuery, String testTableQuery)
    {
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertTrue(testResult.getRowCount() > expectedResult.getRowCount());
        // check that the non-null elements are equal in both tables
        for (int i = 0; i < expectedResult.getRowCount(); i++) {
            assertEquals(expectedResult.getMaterializedRows().get(i),
                    testResult.getMaterializedRows().get(i));
        }
        // check that the remaining elements in the test table are null
        for (int i = expectedResult.getRowCount(); i < testResult.getRowCount(); i++) {
            MaterializedRow row = testResult.getMaterializedRows().get(i);
            for (Object field : row.getFields()) {
                assertNull(field);
            }
        }
    }

    @Test
    public void readTableAllColumnsAfterIncrementalUpdateTest()
    {
        String testTableQuery =
                format("SELECT * FROM \"%s\".\"%s\" order by date, city asc", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                targetTableName));
        String controlTableQuery =
                format("SELECT * FROM \"%s\".\"%s\" order by date, city asc", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                controlTableName));
        checkQueryOutputOnIncrementalWithNullRows(controlTableQuery, testTableQuery);
    }

    @Test
    public void readTableAllColumnsAfterIncrementalUpdateFilteringNullsTest()
    {
        String testTableQuery =
                format("SELECT * FROM \"%s\".\"%s\" where name is not null order by date, city asc",
                        PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        targetTableName));
        String controlTableQuery =
                format("SELECT * FROM \"%s\".\"%s\" where name is not null order by date, city asc",
                        PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        controlTableName));
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertEquals(testResult.getMaterializedRows(), expectedResult.getMaterializedRows());
    }

    @Test
    public void readTableNonePartitionedColumnAfterIncrementalUpdateTest()
    {
        String testTableQuery =
                format("SELECT name, cnt FROM \"%s\".\"%s\" order by name, cnt asc", PATH_SCHEMA,
                        goldenTablePathWithPrefix(version, targetTableName));
        String controlTableQuery =
                format("SELECT name, cnt FROM \"%s\".\"%s\" order by name, cnt asc", PATH_SCHEMA,
                        goldenTablePathWithPrefix(version, controlTableName));
        checkQueryOutputOnIncrementalWithNullRows(controlTableQuery, testTableQuery);
    }

    @Test
    public void readTableNonePartitionedColumnAfterIncrementalUpdateFilteringNullsTest()
    {
        String testTableQuery =
                format("SELECT name, cnt FROM \"%s\".\"%s\" where name is not null and " +
                 "cnt is not null order by name, cnt asc", PATH_SCHEMA,
                 goldenTablePathWithPrefix(version, targetTableName));
        String controlTableQuery =
                format("SELECT name, cnt FROM \"%s\".\"%s\" where name is not null and " +
                 "cnt is not null order by name, cnt asc", PATH_SCHEMA,
                 goldenTablePathWithPrefix(version, controlTableName));
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertEquals(testResult.getMaterializedRows(), expectedResult.getMaterializedRows());
    }

    @Test
    public void readTablePartitionedColumnAfterIncrementalUpdateTest()
    {
        String testTableQuery =
                format("SELECT date, city FROM \"%s\".\"%s\" order by date, city asc", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        targetTableName));
        String controlTableQuery =
                format("SELECT date, city FROM \"%s\".\"%s\" order by date, city asc", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        controlTableName));
        checkQueryOutputOnIncrementalWithNullRows(controlTableQuery, testTableQuery);
    }

    @Test
    public void readTablePartitionedColumnFilteringNullValuesAfterIncrementalUpdateTest()
    {
        String testTableQuery =
                format("SELECT date FROM \"%s\".\"%s\" where date is not null order by date asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        targetTableName));
        String controlTableQuery =
                format("SELECT date FROM \"%s\".\"%s\" where date is not null order by date asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        controlTableName));
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertEquals(testResult.getMaterializedRows(), expectedResult.getMaterializedRows());
    }
}
