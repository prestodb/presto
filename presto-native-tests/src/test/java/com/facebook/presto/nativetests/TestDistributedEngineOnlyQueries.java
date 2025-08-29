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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.parseBoolean;

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
        assertQueryFails("SELECT TIME '3:04:05'", timeTypeUnsupportedError);
        assertQueryFails("SELECT TIME '3:04:05.123'", timeTypeUnsupportedError);
        assertQueryFails("SELECT TIME '3:04:05'", timeTypeUnsupportedError);
        assertQueryFails("SELECT TIME '0:04:05'", timeTypeUnsupportedError);
        // TODO #7122 assertQueryFails(chicago, "SELECT TIME '3:04:05'", timeTypeUnsupportedError);
        // TODO #7122 assertQueryFails(kathmandu, "SELECT TIME '3:04:05'", timeTypeUnsupportedError);

        assertQueryFails("SELECT TIME '01:02:03.400 Z'", timeTypeUnsupportedError);
        assertQueryFails("SELECT TIME '01:02:03.400 UTC'", timeTypeUnsupportedError);
        assertQueryFails("SELECT TIME '3:04:05 +06:00'", timeTypeUnsupportedError);
        assertQueryFails("SELECT TIME '3:04:05 +0507'", timeTypeUnsupportedError);
        assertQueryFails("SELECT TIME '3:04:05 +03'", timeTypeUnsupportedError);
    }

    /// TIME datatype is not supported in Prestissimo. See issue: https://github.com/prestodb/presto/issues/18844.
    @Override
    @Test
    public void testLocallyUnrepresentableTimeLiterals()
    {
        LocalTime localTimeThatDidNotOccurOn20120401 = LocalTime.of(2, 10);
        checkState(ZoneId.systemDefault().getRules().getValidOffsets(localTimeThatDidNotOccurOn20120401.atDate(LocalDate.of(2012, 4, 1))).isEmpty(), "This test assumes certain JVM time zone");
        @Language("SQL") String sql = DateTimeFormatter.ofPattern("'SELECT TIME '''HH:mm:ss''").format(localTimeThatDidNotOccurOn20120401);
        assertQueryFails(sql, timeTypeUnsupportedError);
    }

    // todo: turn on these test cases when the sql invoked functions are extracted into a plugin module.
    @Override
    @Test(enabled = false)
    public void testArraySplitIntoChunks()
    {
    }

    @Override
    @Test(enabled = false)
    public void testCrossJoinWithArrayNotContainsCondition()
    {
    }

    @Override
    @Test(enabled = false)
    public void testSamplingJoinChain()
    {
    }

    @Override
    @Test(enabled = false)
    public void testKeyBasedSampling()
    {
    }

    @Override
    @Test(enabled = false)
    public void testDefaultSamplingPercent()
    {
    }

    @Override
    @Test(enabled = false)
    public void testLeftJoinWithArrayContainsCondition()
    {
    }

    @Override
    @Test(enabled = false)
    public void testTry()
    {
    }
}
