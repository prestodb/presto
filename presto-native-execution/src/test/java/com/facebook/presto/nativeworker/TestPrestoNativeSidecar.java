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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.setupNativeFunctionNamespaceManager;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.setupSessionPropertyProvider;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
@Test(singleThreaded = true)
public class TestPrestoNativeSidecar
        extends AbstractTestQueryFramework
{
    private static final String REGEX_FUNCTION_NAMESPACE = "native.default.*";
    private static final String REGEX_SESSION_NAMESPACE = "Native Execution only.*";
    private static final int FUNCTION_COUNT = 1113;

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createOrders(queryRunner);
        createLineitem(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.createQueryRunnerWithSidecar(false);
        setupNativeFunctionNamespaceManager(queryRunner, "native");
        setupSessionPropertyProvider(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Test
    public void testShowFunctions()
    {
        String sql = "SHOW FUNCTIONS";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        List<MaterializedRow> filteredRows = specialFunctionsToExcludeFilter(actualRows);
        assertTrue(filteredRows.size() >= FUNCTION_COUNT);
        filteredRows.forEach(row -> {
            // Iterate over all cells in the row
            for (Object cellValue : row.getFields()) {
                if (Pattern.matches(REGEX_FUNCTION_NAMESPACE, cellValue.toString())) {
                    return;
                }
            }
            fail(format("no namespace match found for row: %s", row));
        });
    }
    private List<MaterializedRow> specialFunctionsToExcludeFilter(List<MaterializedRow> inputRows)
    {
        return inputRows.stream()
                .filter(row -> !row.getFields().contains("is_null") &&
                        !row.getFields().contains("in"))
                .collect(toImmutableList());
    }

    private List<MaterializedRow> excludeJavaSessionProperties(List<MaterializedRow> inputRows)
    {
        return inputRows.stream()
                .filter(row -> Pattern.matches(REGEX_SESSION_NAMESPACE, row.getFields().get(4).toString()))
                .collect(Collectors.toList());
    }

    @Test
    public void testShowSession()
    {
        @Language("SQL") String sql = "SHOW SESSION";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        List<MaterializedRow> filteredRows = excludeJavaSessionProperties(actualRows);
        assertTrue(filteredRows.size() > 0);
    }

    @Test
    public void testJavaWorkerProperties()
    {
        assertQueryFails("SET SESSION aggregation_spill_enabled=false", "line 1:1: Session property aggregation_spill_enabled does not exist");
    }

    @Test
    public void testSetNativeSessionProperty()
    {
        @Language("SQL") String setSession = "SET SESSION driver_cpu_time_slice_limit_ms=500";
        MaterializedResult setSessionResult = computeActual(setSession);
        assertEquals(
                setSessionResult.toString(),
                "MaterializedResult{rows=[[true]], " +
                        "types=[boolean], " +
                        "setSessionProperties={driver_cpu_time_slice_limit_ms=500}, " +
                        "resetSessionProperties=[], updateType=SET SESSION}");
    }
}
