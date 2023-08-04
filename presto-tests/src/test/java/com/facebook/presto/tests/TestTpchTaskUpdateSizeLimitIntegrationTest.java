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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestTpchTaskUpdateSizeLimitIntegrationTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.createQueryRunner(ImmutableMap.of("experimental.internal-communication.max-task-update-size", "100B"));
    }

    // This test is flaky: https://github.com/prestodb/presto/issues/19659
    @Test(enabled = false)
    public void testTaskUpdateSizeLimit()
    {
        assertQueryFails(
                getSession(),
                "SELECT * FROM ORDERS LIMIT 10",
                "TaskUpdate size of [0-9]* Bytes has exceeded the limit of 100 Bytes");
    }
}
