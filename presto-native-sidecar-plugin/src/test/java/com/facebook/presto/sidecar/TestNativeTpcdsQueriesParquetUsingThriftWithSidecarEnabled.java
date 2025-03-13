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

import com.facebook.presto.nativeworker.AbstractTestNativeTpcdsQueries;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

public class TestNativeTpcdsQueriesParquetUsingThriftWithSidecarEnabled
        extends AbstractTestNativeTpcdsQueries
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
        this.storageFormat = "PARQUET";
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner("PARQUET");
    }

    @Override
    @Test
    public void testTpcdsQ2()
            throws Exception
    {
        assertQueryFails(session, getTpcdsQuery("02"), "Variable is not bound: i2");
    }

    @Override
    @Test
    public void testTpcdsQ78()
            throws Exception
    {
        assertQueryFails(session, getTpcdsQuery("78"), "Variable is not bound: i2");
    }
}
