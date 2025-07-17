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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static java.lang.Boolean.parseBoolean;
import static org.testng.Assert.assertEquals;

public class TestNowScalarFunction
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;
    QueryRunner queryRunner;

    @BeforeClass
    @Override
    public void init() throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        queryRunner = NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
        return queryRunner;
    }

    // Enable the test case once now() function is merged in velox :  https://github.com/facebookincubator/velox/pull/14139
    @Test(enabled = false)
    public void testNowAndCurrentTimestampAreSynonyms()
    {
        assertQuery("SELECT now() = current_timestamp", "SELECT true");
    }

    @Test(enabled = false)
    public void testConsistencyWithinQuery()
    {
        assertQuery("SELECT now() = now(), current_timestamp = current_timestamp", "SELECT true, true");
    }
}
