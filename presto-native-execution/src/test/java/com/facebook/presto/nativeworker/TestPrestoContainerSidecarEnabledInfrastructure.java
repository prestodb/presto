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
package com.facebook.presto.nativeworker;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public class TestPrestoContainerSidecarEnabledInfrastructure
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner(8080, "tpch", "tiny", 4, true, true);
    }

    @Override
    protected ContainerQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner(8080, "tpch", "tiny", 4, false, true);
    }

    @Test
    public void TestNativeCluster()
            throws Exception
    {
        assertQuerySucceeds("SHOW FUNCTIONS");
        assertQueryFails("SELECT fail('forced failure')", "(?s).*Top-level Expression: native\\.default\\.fail\\(forced failure:VARCHAR\\).*", true);
        assertQuerySucceeds("SHOW SESSION");
        assertQuerySucceeds("select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])");
//        Remove comment once now() function is merged in velox : https://github.com/facebookincubator/velox/pull/14139
//        assertQuerySucceeds("SELECT now()");
    }

    @Test
    public void TestJavaCluster()
            throws Exception
    {
        assertQueryFailsExpected("SHOW FUNCTIONS", "Query failed .*.: Function namespace not found for catalog: native", true);
        assertQueryFailsExpected("select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", "Query failed .*.: Function native.default.array_sort not registered", true);
    }
}
