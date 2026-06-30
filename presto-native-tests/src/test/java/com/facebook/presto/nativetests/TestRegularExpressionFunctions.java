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

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestRegularExpressionFunctions
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
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createNation(queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        QueryRunner expectedQueryRunner = (QueryRunner) getExpectedQueryRunner();
        expectedQueryRunner.execute("DROP TABLE IF EXISTS nation");
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
    public void testRegexpLike()
    {
        assertQuery("SELECT regexp_like(concat(name, '-', comment), '.*ALGERIA.*') FROM nation");
        assertQuery("SELECT regexp_like(name, 'A.*') FROM nation");
        assertQuery("SELECT regexp_like(name, '^A.*A$') FROM nation");
        assertQuery("SELECT regexp_like(name, '^[a-z]+$') FROM nation");
        assertQuery("SELECT regexp_like(name, '^(?i)[a-z]+$') FROM nation");
        assertQuery("SELECT regexp_like(comment, '.*haggle.*') FROM nation");
        assertQuery("SELECT regexp_like(comment, '.*\\bhaggle\\b.*') FROM nation");
        assertQuery("SELECT regexp_like(comment, '.*\\bdependencies\\b.*') FROM nation");
        assertQuery("SELECT regexp_like(concat(name, '.', cast(regionkey AS VARCHAR), '.', cast(nationkey AS VARCHAR)), '\\.1\\.') FROM nation");
        assertQuery("SELECT regexp_like(concat(name, '\n', comment), '.*\\n.*') FROM nation");
        assertQuery("SELECT regexp_like(IF(nationkey = 0, CAST(NULL AS VARCHAR), name), 'A.*') FROM nation");
        assertQuery("SELECT regexp_like('', '') FROM nation");
        assertQuery("SELECT regexp_like(substr(name, 1, 0), '') FROM nation");
        assertQuery("SELECT regexp_like(name, '') FROM nation");
        assertQuery("SELECT regexp_like('', 'A.*') FROM nation");
        assertQuery("SELECT regexp_like(substr(name, 1, 0), 'A.*') FROM nation");
        assertQuery("SELECT regexp_like(name, 'ALGERIA|BRAZIL|CHINA') FROM nation");
        assertQuery("SELECT regexp_like(comment, 'final|requests|pending') FROM nation");
        assertQuery("SELECT regexp_like(name, '^A.*|.*A$') FROM nation");
        assertQuery("SELECT regexp_like(name, '^UNITED') FROM nation");
        assertQuery("SELECT regexp_like(name, 'IA$') FROM nation");
        assertQuery("SELECT regexp_like(name, '^ALGERIA$') FROM nation");
        assertQuery("SELECT regexp_like(comment, '^the') FROM nation");
        assertQuery("SELECT regexp_like(comment, 'ly$') FROM nation");

        assertQueryFails(
                "SELECT regexp_like(name, '(') FROM nation",
                ".*missing.*\\).*");
        assertQueryFails(
                "SELECT regexp_like(comment, '[') FROM nation",
                ".*missing.*\\].*");
        assertQueryFails(
                "SELECT regexp_like(name, '*a') FROM nation",
                ".*(?:invalid regular expression|error parsing regexp).*");
    }
}
