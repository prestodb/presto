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
package com.facebook.presto.nativetests.cudf;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

@Test(singleThreaded = true)
public class TestCudfSidecarPlugin
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createOrders(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        if (!isCudfTestEnabled()) {
            throw new SkipException("cuDF tests disabled. Set -Dcudf-tests-enabled=true to enable.");
        }

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setCoordinatorSidecarEnabled(true)
                .setCudfEnabled(true)
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8089"))
                .build();
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder().build();
    }

    @Test
    public void testCudfPlanCheckerRejectsUnsupportedPlan()
    {
        assertQueryFails(
                "SELECT count(DISTINCT orderkey) FROM orders",
                "Cudf plan checker");
    }

    private static boolean isCudfTestEnabled()
    {
        String propertyValue = System.getProperty("cudf-tests-enabled");
        if (propertyValue != null) {
            return Boolean.parseBoolean(propertyValue);
        }
        String envValue = System.getenv("CUDF_TESTS_ENABLED");
        return envValue != null && Boolean.parseBoolean(envValue);
    }
}
