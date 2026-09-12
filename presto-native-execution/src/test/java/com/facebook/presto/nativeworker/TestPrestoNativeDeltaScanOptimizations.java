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

import com.facebook.airlift.log.Level;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.delta.TestDeltaScanOptimizations;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestPrestoNativeDeltaScanOptimizations
        extends TestDeltaScanOptimizations
{
    @Override
    protected String goldenTablePath(String tableName)
    {
        return extractedGoldenTablePath(tableName);
    }

    @BeforeClass
    public static void silenceDeltaLogging()
    {
        // Hide huge warning logs caused by not having checkpoints.
        Logging logging = Logging.initialize();
        logging.setLevel("io.delta.kernel", Level.ERROR);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeDeltaQueryRunnerBuilder()
                .build();
        // Create the test Delta tables in HMS
        for (String deltaTestTable : DELTA_TEST_TABLE_LIST) {
            registerDeltaTableInHMS(queryRunner, deltaTestTable, deltaTestTable);
        }
        return queryRunner;
    }

    @Override
    @Test(dataProvider = "deltaReaderVersions")
    public void nestedColumnFilter(String version)
    {
        // Native execution does not yet support nested column filter pushdown to Delta table layout.
        // For now, we only verify the query produces correct results without checking plan optimization.
        String tableName = getVersionPrefix(version) + "data-reader-nested-struct";
        String testQuery = format("SELECT a.aa, a.ac.aca FROM \"%s\" WHERE a.aa in ('8', '9') AND a.ac.aca > 6",
                tableName);
        String expResultsQuery = "SELECT * FROM VALUES('8', 8),('9', 9)";

        // Only verify query results, skip plan verification
        assertQuery(testQuery, expResultsQuery);
    }
}
