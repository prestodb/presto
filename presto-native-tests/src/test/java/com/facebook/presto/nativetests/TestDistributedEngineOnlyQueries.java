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
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.parseBoolean;

public class TestDistributedEngineOnlyQueries
        extends AbstractTestEngineOnlyQueries
{
    private static final String timeTypeUnsupportedErrorWithoutSidecar = ".*Failed to parse type \\[time.*";
    private static final String timeTypeUnsupportedErrorWithSidecar = "^Unknown type time.*";
    private String timeTypeUnsupportedError = timeTypeUnsupportedErrorWithoutSidecar;

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
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        if (sidecarEnabled) {
            timeTypeUnsupportedError = timeTypeUnsupportedErrorWithSidecar;
        }
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled, Optional.empty());
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
        LocalTime localTimeThatDidNotOccurOn19700101 = LocalTime.of(0, 10);
        checkState(ZoneId.systemDefault().getRules().getValidOffsets(localTimeThatDidNotOccurOn19700101.atDate(LocalDate.ofEpochDay(0))).isEmpty(), "This test assumes certain JVM time zone");
        checkState(!Objects.equals(java.sql.Time.valueOf(localTimeThatDidNotOccurOn19700101).toLocalTime(), localTimeThatDidNotOccurOn19700101), "This test assumes certain JVM time zone");
        @Language("SQL") String sql = DateTimeFormatter.ofPattern("'SELECT TIME '''HH:mm:ss''").format(localTimeThatDidNotOccurOn19700101);
        assertQueryFails(sql, timeTypeUnsupportedError);
    }
}
