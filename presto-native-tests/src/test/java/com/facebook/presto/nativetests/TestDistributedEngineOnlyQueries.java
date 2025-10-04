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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestEngineOnlyQueries;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalTime;

import static java.lang.Boolean.parseBoolean;
import static org.testng.Assert.assertEquals;

public class TestDistributedEngineOnlyQueries
        extends AbstractTestEngineOnlyQueries
{
    private String timeTypeUnsupportedError;
    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        if (sidecarEnabled) {
            timeTypeUnsupportedError = "^Unknown type time.*";
        }
        else {
            timeTypeUnsupportedError = ".*Failed to parse type \\[time.*";
        }
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    /// TIME datatype is not supported in Prestissimo. See issue: https://github.com/prestodb/presto/issues/18844.
    @Override
    @Test
    public void testTimeLiterals()
    {
        assertEquals(computeScalar("SELECT TIME '3:04:05'"), LocalTime.of(3, 4, 5, 0));
        assertEquals(computeScalar("SELECT TIME '3:04:05.123'"), LocalTime.of(3, 4, 5, 123_000_000));
        assertQuery("SELECT TIME '3:04:05'");
        assertQuery("SELECT TIME '0:04:05'");
        // TODO #7122 assertQueryFails(chicago, "SELECT TIME '3:04:05'", timeTypeUnsupportedError);
        // TODO #7122 assertQueryFails(kathmandu, "SELECT TIME '3:04:05'", timeTypeUnsupportedError);

        // TODO: re-enable the timestamp related test failures later.
        //assertQueryFails("SELECT TIME '01:02:03.400 Z'", timeTypeUnsupportedError);
        //assertQueryFails("SELECT TIME '01:02:03.400 UTC'", timeTypeUnsupportedError);
        //assertQueryFails("SELECT TIME '3:04:05 +06:00'", timeTypeUnsupportedError);
        //assertQueryFails("SELECT TIME '3:04:05 +0507'", timeTypeUnsupportedError);
        //assertQueryFails("SELECT TIME '3:04:05 +03'", timeTypeUnsupportedError);
    }
}
