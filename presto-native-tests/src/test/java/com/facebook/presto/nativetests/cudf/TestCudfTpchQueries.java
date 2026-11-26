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

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.nativetests.NativeTestsUtils;
import com.facebook.presto.nativeworker.AbstractTestNativeTpchQueries;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sessionpropertyproviders.NativeWorkerSessionPropertyProvider.NATIVE_CUDF_ENABLED;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCudfTpchQueries
        extends AbstractTestNativeTpchQueries
{
    private static final String DEFAULT_STORAGE_FORMAT = "PARQUET";
    private static final int TPCH_QUERY_COUNT = 22;

    @Override
    protected String getStorageFormat()
    {
        return System.getProperty("storageFormat", DEFAULT_STORAGE_FORMAT);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(getStorageFormat());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(getStorageFormat())
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(false)
                .setEnableCudf(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(getStorageFormat())
                .setAddStorageFormatToPath(true)
                .build();
    }

    @DataProvider(name = "tpchQueryNumbers")
    public Object[][] tpchQueryNumbers()
    {
        Object[][] queryNumbers = new Object[TPCH_QUERY_COUNT][1];
        for (int q = 1; q <= TPCH_QUERY_COUNT; q++) {
            queryNumbers[q - 1][0] = q;
        }
        return queryNumbers;
    }

    /**
     * Runs one TPC-H query on the same GPU-enabled cluster with and without
     * {@code native_cudf_enabled}. The two runs must agree on the results, and only the
     * enabled run may contain cuDF operators: a disabled run that still shows them would
     * mean the property reached the coordinator but not the worker's execution.
     */
    @Test(dataProvider = "tpchQueryNumbers")
    public void testCudfEnabledSessionProperty(int queryNumber)
            throws Exception
    {
        String sql = getTpchQuery(queryNumber);
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        ResultWithQueryId<MaterializedResult> enabled =
                queryRunner.executeWithQueryId(sessionWithCudfEnabled(true), sql);
        ResultWithQueryId<MaterializedResult> disabled =
                queryRunner.executeWithQueryId(sessionWithCudfEnabled(false), sql);

        assertEqualsIgnoreOrder(
                disabled.getResult().getMaterializedRows(),
                enabled.getResult().getMaterializedRows());

        List<String> enabledOperators = cudfOperatorTypes(queryRunner.getQueryInfo(enabled.getQueryId()));
        assertFalse(enabledOperators.isEmpty(), format("Expected cuDF operators with %s=true", NATIVE_CUDF_ENABLED));

        List<String> disabledOperators = cudfOperatorTypes(queryRunner.getQueryInfo(disabled.getQueryId()));
        assertTrue(
                disabledOperators.isEmpty(),
                format("Expected no cuDF operators with %s=false, found %s", NATIVE_CUDF_ENABLED, disabledOperators));
    }

    private Session sessionWithCudfEnabled(boolean enabled)
    {
        return Session.builder(getSession())
                .setSystemProperty(NATIVE_CUDF_ENABLED, Boolean.toString(enabled))
                .build();
    }

    private static List<String> cudfOperatorTypes(QueryInfo queryInfo)
    {
        return queryInfo.getOutputStage()
                .map(StageInfo::getAllStages)
                .orElseThrow(() -> new AssertionError("Query ran without an output stage"))
                .stream()
                .flatMap(stage -> stage.getLatestAttemptExecutionInfo().getTasks().stream())
                .map(TaskInfo::getStats)
                .flatMap(stats -> stats.getPipelines().stream())
                .flatMap(pipeline -> pipeline.getOperatorSummaries().stream())
                .map(operator -> operator.getOperatorType())
                .filter(operatorType -> operatorType.startsWith("Cudf"))
                .distinct()
                .collect(toImmutableList());
    }
}
