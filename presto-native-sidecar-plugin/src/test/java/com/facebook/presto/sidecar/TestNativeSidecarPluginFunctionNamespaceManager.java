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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestNativeSidecarPluginFunctionNamespaceManager
        extends AbstractTestQueryFramework
{
    private static final String REGEX_FUNCTION_NAMESPACE = "native.default.*";
    private static final int FUNCTION_COUNT = 1375;

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createNation(queryRunner);
        createOrders(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.createQueryRunner(false, true);
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Test
    public void testShowFunctions()
    {
        @Language("SQL") String sql = "SHOW FUNCTIONS";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        assertEquals(actualRows.size(), FUNCTION_COUNT);
        for (MaterializedRow actualRow : actualRows) {
            List<Object> row = actualRow.getFields();
            String functionName = row.get(0).toString();
            if (Pattern.matches(REGEX_FUNCTION_NAMESPACE, functionName)) {
                continue;
            }
            fail(format("No namespace match found for row: %s", row));
        }
    }

    @Test
    public void testBasicQueries()
    {
        assertQuery("select corr(nation.nationkey, nation.nationkey) from nation");
        assertQuery("select count(comment) from orders");
        assertQuery("select count(*) from nation");
        assertQuery("select lower(comment) from nation");
        assertQuery("select array[nationkey], array_constructor(comment) from nation");
        assertQuery("select count(abs(orderkey) between 1 and 60000) from orders group by orderkey");
    }
}
