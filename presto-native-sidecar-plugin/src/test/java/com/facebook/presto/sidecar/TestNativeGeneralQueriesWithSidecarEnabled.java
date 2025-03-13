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
package com.facebook.presto.sidecar;

import com.facebook.presto.nativeworker.AbstractTestNativeGeneralQueries;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

public class TestNativeGeneralQueriesWithSidecarEnabled
        extends AbstractTestNativeGeneralQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true, "PARQUET", true);
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Override
    @Test
    public void testInformationSchemaTables()
    {
        assertQueryFails("select lower(table_name) from information_schema.tables "
                        + "where table_name = 'lineitem' or table_name = 'LINEITEM' ",
                "Compiler failed");
    }

    // Fails due to error: resolved function input type does not match the input type: varchar != varchar(25)
    @Override
    @Test(enabled = false)
    public void testSystemTables() {}

    // Fails due to error: resolved function input type does not match the input type: varchar != varchar(25)
    @Override
    @Test(enabled = false)
    public void testAnalyzeStats() {}

    // SHOW commands will return different outputs
    @Override
    @Test
    public void testShowAndDescribe()
    {
        assertQuerySucceeds("SHOW functions");
        assertQuerySucceeds("SHOW tables");
        assertQuerySucceeds("DESCRIBE lineitem");
    }

    // set session on a java worker session property won't work as we are enabling the config flag
    // `exclude-invalid-worker-session-properties`.
    @Override
    @Test(enabled = false)
    public void testSetSessionJavaWorkerSessionProperty() {}
}
