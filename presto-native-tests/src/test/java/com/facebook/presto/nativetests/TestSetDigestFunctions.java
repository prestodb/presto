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
package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativetests.SetDigestTestUtils.createJaccardIndexValues;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestSetDigestFunctions
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        super.init();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createJaccardIndexValues(queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        QueryRunner expectedQueryRunner = (QueryRunner) getExpectedQueryRunner();
        expectedQueryRunner.execute("DROP TABLE IF EXISTS jaccardIndexTable");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testJaccardIndex()
    {
        assertQuery("SELECT jaccard_index(make_set_digest(v1), make_set_digest(v2)) FROM jaccardIndexTable");
        assertQuery("SELECT jaccard_index(khyperloglog_agg(v1, u1), khyperloglog_agg(v2, u2)) FROM jaccardIndexTable");
        assertQuery("WITH t AS ( SELECT v1, v2, make_set_digest(v1) OVER () AS sd1_all, make_set_digest(v2) OVER () AS sd2_all FROM jaccardIndexTable ) SELECT v1, v2, jaccard_index( IF(v1 IS NULL, CAST(NULL AS setdigest), sd1_all), IF(v2 IS NULL, CAST(NULL AS setdigest), sd2_all)) AS j FROM t")
        assertQueryFails("SELECT jaccard_index(v1, v2) FROM (VALUES ('invalid', 'invalid')) T(v1,v2)", "(?i)line 1:8: Unexpected parameters \\(varchar\\(7\\), varchar\\(7\\)\\) for function (native\\.default\\.)?jaccard_index\\. Expected: .*jaccard_index\\((setdigest|khyperloglog), (setdigest|khyperloglog)\\).*");
    }
}
