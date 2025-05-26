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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestPrestoContainerSidecarInfrastructure
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunner createQueryRunner() throws IOException, InterruptedException, TimeoutException {
        return null;
    }

    @Test
    public void TestNativeClusterWithSidecar()
            throws Exception
    {
        QueryRunner queryRunner = new ContainerQueryRunner(4, true, true, false);
        computeActualWithCustomQueryRunner(queryRunner, "SELECT * FROM system.runtime.nodes");
        computeActualWithCustomQueryRunner(queryRunner, "SHOW FUNCTIONS");
        assertQueryFailsWithCustomQueryRunner(queryRunner,"SELECT fail('forced failure')", "(?s).*Top-level Expression: native\\.default\\.fail\\(forced failure:VARCHAR\\).*", true);
        computeActualWithCustomQueryRunner(queryRunner,"SHOW SESSION");
        computeActualWithCustomQueryRunner(queryRunner,"select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])");
        queryRunner.close();
    }

    @Test
    public void TestNativeClusterWithoutSidecar()
            throws Exception
    {
        QueryRunner queryRunner = new ContainerQueryRunner(4, true, false, false);
        computeActualWithCustomQueryRunner(queryRunner, "SHOW FUNCTIONS");
        assertQueryFailsWithCustomQueryRunner(queryRunner, "SELECT fail('forced failure')", "(?s).*Top-level Expression: presto\\.default\\.fail\\(forced failure:VARCHAR\\).*", true);
        computeActualWithCustomQueryRunner(queryRunner, "SHOW SESSION");
        assertQueryFailsWithCustomQueryRunner(queryRunner, "select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", ".*Expected a lambda that takes 2 argument\\(s\\) but got 1.*", true);
        queryRunner.close();
    }

    @Test
    public void TestNativeClusterWithDelayedSidecar()
            throws Exception
    {
        QueryRunner queryRunner = new ContainerQueryRunner(4, true, true, true);
        assertQueryFailsWithCustomQueryRunner(queryRunner, "SHOW FUNCTIONS", "Query failed .*.: Failed to get functions from sidecar.", true);
        TimeUnit.SECONDS.sleep(60);
        computeActualWithCustomQueryRunner(queryRunner, "SELECT * FROM system.runtime.nodes");
        computeActualWithCustomQueryRunner(queryRunner, "SHOW FUNCTIONS");
        assertQueryFailsWithCustomQueryRunner(queryRunner, "SELECT fail('forced failure')", "(?s).*Top-level Expression: native\\.default\\.fail\\(forced failure:VARCHAR\\).*", true);
        computeActualWithCustomQueryRunner(queryRunner, "SHOW SESSION");
        computeActualWithCustomQueryRunner(queryRunner,"select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])");
        queryRunner.close();
    }

    @Test
    public void TestJavaClusterWithSidecar()
            throws Exception
    {
        QueryRunner queryRunner = new ContainerQueryRunner(4, false, true, false);
        assertQueryFailsWithCustomQueryRunner(queryRunner, "SHOW FUNCTIONS","Query failed .*.: Cannot find function namespace for 'native.default'", true);
        assertQueryFailsWithCustomQueryRunner(queryRunner, "select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", "Query failed .*.: Cannot find function namespace for 'native.default'", true);
        queryRunner.close();
    }

    @Test
    public void TestJavaClusterWithoutSidecar()
            throws Exception
    {
        QueryRunner queryRunner = new ContainerQueryRunner(4, false, false, false);
        computeActualWithCustomQueryRunner(queryRunner, "SHOW FUNCTIONS");
        assertQueryFailsWithCustomQueryRunner(queryRunner, "SELECT fail('forced failure')", "Query failed .*.: forced failure", true);
        computeActualWithCustomQueryRunner(queryRunner, "SHOW SESSION");
        assertQueryFailsWithCustomQueryRunner(queryRunner, "select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])", ".*Expected a lambda that takes 2 argument\\(s\\) but got 1.*", true);
        queryRunner.close();
    }
}
