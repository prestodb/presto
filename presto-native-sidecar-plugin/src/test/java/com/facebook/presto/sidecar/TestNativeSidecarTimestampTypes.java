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

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

/**
 * End-to-end native sidecar tests for TIMESTAMP type handling.
 */
public class TestNativeSidecarTimestampTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        TestNativeSidecarPlugin.setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testPreEpochTimestampDecomposition()
    {
        assertQuery("SELECT year(TIMESTAMP '1969-12-31 23:59:59.999')");
        assertQuery("SELECT month(TIMESTAMP '1969-12-31 23:59:59.999')");
        assertQuery("SELECT day(TIMESTAMP '1969-12-31 23:59:59.999')");
        assertQuery("SELECT hour(TIMESTAMP '1969-12-31 23:59:59.999')");
        assertQuery("SELECT minute(TIMESTAMP '1969-12-31 23:59:59.999')");
        assertQuery("SELECT second(TIMESTAMP '1969-12-31 23:59:59.999')");
        assertQuery("SELECT millisecond(TIMESTAMP '1969-12-31 23:59:59.999')");
    }

    @Test
    public void testPreEpochTimestampToUnixtime()
    {
        assertQuery("SELECT to_unixtime(TIMESTAMP '1969-12-31 23:59:59.999')");
        assertQuery("SELECT to_unixtime(TIMESTAMP '1969-12-31 23:59:59.000')");
        assertQuery("SELECT to_unixtime(TIMESTAMP '1969-12-31 23:59:58.999')");
        assertQuery("SELECT to_unixtime(TIMESTAMP '1970-01-01 00:00:00.000')");
    }

    @Test
    public void testPreEpochTimestampArithmetic()
    {
        assertQuery("SELECT date_diff('millisecond', TIMESTAMP '1969-12-31 23:59:59.000', TIMESTAMP '1970-01-01 00:00:01.000')");
        assertQuery("SELECT date_add('millisecond', -1, TIMESTAMP '1970-01-01 00:00:00.000')");
        assertQuery("SELECT date_add('day', -1, TIMESTAMP '1970-01-01 00:00:00.000')");
    }

    @Test
    public void testParameterizedTimestampNotSupported()
    {
        String[][] cases = {
                {"0", "2021-01-01 00:00:00"},
                {"1", "2021-01-01 00:00:00.1"},
                {"2", "2021-01-01 00:00:00.12"},
                {"3", "2021-01-01 00:00:00.123"},
                {"4", "2021-01-01 00:00:00.1234"},
                {"5", "2021-01-01 00:00:00.12345"},
                {"6", "2021-01-01 00:00:00.123456"},
                {"7", "2021-01-01 00:00:00.1234567"},
                {"9", "2021-01-01 00:00:00.123456789"},
                {"12", "2021-01-01 00:00:00.123456789012"},
        };
        for (String[] c : cases) {
            assertQueryFails(
                    "SELECT CAST('" + c[1] + "' AS TIMESTAMP(" + c[0] + "))",
                    "line 1:8: Unknown type: timestamp\\(" + c[0] + "\\)");
        }
    }
}
