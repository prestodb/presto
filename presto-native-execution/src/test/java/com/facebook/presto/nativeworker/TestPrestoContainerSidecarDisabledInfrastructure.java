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

public class TestPrestoContainerSidecarDisabledInfrastructure
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner(8080, "tpch", "tiny", 4, true, false, 0, false);
    }

    @Override
    protected ContainerQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner(8080, "tpch", "tiny", 4, false, false, 0, false);
    }

    @Test
    public void TestNativeCluster()
            throws Exception
    {
        assertQuerySucceeds("SHOW FUNCTIONS");
        assertQueryFails("SELECT fail('forced failure')", "(?s).*Top-level Expression: presto\\.default\\.fail\\(forced failure:VARCHAR\\).*", true);
        assertQuerySucceeds("SHOW SESSION");
        assertQueryFails("select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", ".*Expected a lambda that takes 2 argument\\(s\\) but got 1.*", true);
    }

    @Test
    public void TestJavaCluster()
            throws Exception
    {
        assertQuerySucceedsExpected("SHOW FUNCTIONS");
        assertQueryFailsExpected("SELECT fail('forced failure')", "Query failed .*.: forced failure", true);
        assertQuerySucceedsExpected("SHOW SESSION");
        assertQueryFailsExpected("select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", ".*Expected a lambda that takes 2 argument\\(s\\) but got 1.*", true);
    }
}
