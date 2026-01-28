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
package com.facebook.presto.nativetests.functions;

import com.facebook.presto.nativetests.NativeTestsUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder;
import static java.lang.Boolean.parseBoolean;

public class TestThetaSketchFunctions
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
    protected QueryRunner createQueryRunner() throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaHiveQueryRunnerBuilder()
            .setStorageFormat(storageFormat)
            .build();
    }

    @Test
    public void testSketchThetaSummary()
    {
        assertQuery("SELECT sketch_theta_summary(CAST(NULL as VARBINARY))");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(cast(25 as tinyint)))");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(cast(250 as smallint)))");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(40000))");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(2147483650))");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(cast(214748.3650 as real)))");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(cast(2147483650123283628.72323 as double)))");

        assertQuery("SELECT sketch_theta_summary(sketch_theta('sampletesttext'))");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(date'2025-11-12'))");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(timestamp'2025-11-12 03:47:58'))");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(cast(214748.3650 as DECIMAL(10,4))))");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(cast(2147483650123283628.72323 as DECIMAL(25,5))))");
    }

    @Test
    public void testSketchThetaEstimate()
    {
        assertQuery("SELECT sketch_theta_estimate(CAST(NULL as VARBINARY))");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(cast(25 as tinyint)))");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(cast(250 as smallint)))");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(40000))");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(2147483650))");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(cast(214748.3650 as real)))");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(cast(2147483650123283628.72323 as double)))");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta('sampletesttext'))");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(date'2025-11-12'))");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(timestamp'2025-11-12 03:47:58'))");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(cast(214748.3650 as DECIMAL(10,4))))");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(cast(2147483650123283628.72323 as DECIMAL(25,5))))");
    }
}
