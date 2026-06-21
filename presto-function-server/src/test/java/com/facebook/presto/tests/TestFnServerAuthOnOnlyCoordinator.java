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
package com.facebook.presto.tests;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.utils.FnServerAuthTestUtils;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestFnServerAuthOnOnlyCoordinator
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestFnServerAuthOnOnlyCoordinator.class);

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return FnServerAuthTestUtils.createCoordinatorOnlyRunnerWithMtlsAndJwt();
    }

    @Override
    protected Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("remote_functions_enabled", "true")
                .build();
    }

    /**
     * Tests secure authentication (mTLS + JWT) between Coordinator and Function-Server ONLY.
     * Uses coordinator-only query runner to ensure queries execute on coordinator, not workers.
     */
    @Test
    public void testMtlsJwtBetweenCoordinatorAndFnServer()
    {
        MaterializedResult result = computeActual(
                "SELECT rest.default.abs(-123)");
        assertEquals(result.getOnlyValue(), 123);
        log.info("Coordinator→Function-Server test passed with result value %d", result.getOnlyValue());
    }
}
