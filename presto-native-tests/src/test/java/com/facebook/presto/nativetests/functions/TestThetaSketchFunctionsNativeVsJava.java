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
package com.facebook.presto.nativetests.functions;

import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.nativetests.NativeTestsUtils;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.apache.datasketches.theta.Union;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestThetaSketchFunctionsNativeVsJava
        extends AbstractTestFunctions
{
    private final QueryRunner nativeQueryRunner;
    private final String functionNamespace = "presto.default.";
    TestThetaSketchFunctionsNativeVsJava()
            throws Exception
    {
        super(TEST_SESSION, new FeaturesConfig(), new FunctionsConfig(), false);
        nativeQueryRunner = NativeTestsUtils.createNativeQueryRunner("PARQUET", true);
    }

    @Test
    public void testNullSketch()
    {
        MaterializedResult result = nativeQueryRunner.execute(session, "SELECT " + functionNamespace + "sketch_theta_estimate(CAST(NULL as VARBINARY))");

        assertTrue(result.getOnlyValue() == null);
        functionAssertions.assertFunction("sketch_theta_estimate(CAST(NULL as VARBINARY))", DOUBLE, null);
    }

    @Test
    public void testEstimateEmptySketch()
    {
        Union union = Union.builder().buildUnion();
        String sketchBytes = toVarbinarySql(union.getResult().toByteArray());

        MaterializedResult result = nativeQueryRunner.execute(session, format("SELECT " + functionNamespace + "sketch_theta_estimate(CAST(X'%s' as VARBINARY))", sketchBytes));

        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_estimate(CAST(X'%s' as VARBINARY))",
                        sketchBytes),
                DOUBLE,
                result.getOnlyValue());
    }

    @Test
    public void testEstimateSingleValue()
    {
        Union union = Union.builder().buildUnion();
        union.update(1);
        String sketchBytes = toVarbinarySql(union.getResult().toByteArray());

        MaterializedResult result = nativeQueryRunner.execute(session, format("SELECT " + functionNamespace + "sketch_theta_estimate(CAST(X'%s' as VARBINARY))", sketchBytes));

        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_estimate(CAST(X'%s' as VARBINARY))", sketchBytes),
                DOUBLE,
                result.getOnlyValue());
    }

    @Test
    public void testEstimateManyValues()
    {
        Union union = Union.builder().buildUnion();
        int size = 100;
        for (int i = 0; i < size; i++) {
            union.update(i);
        }
        String sketchBytes = toVarbinarySql(union.getResult().toByteArray());

        MaterializedResult result = nativeQueryRunner.execute(session, format("SELECT " + functionNamespace + "sketch_theta_estimate(CAST(X'%s' as VARBINARY))", sketchBytes));

        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_estimate(CAST(X'%s' as VARBINARY))", sketchBytes),
                DOUBLE,
                result.getOnlyValue());
    }

    @Test
    public void testSummaryNull()
    {
        MaterializedResult result = nativeQueryRunner.execute(session, "SELECT " + functionNamespace + "sketch_theta_summary(CAST(NULL as VARBINARY))");
        assertTrue(result.getOnlyValue() == null);

        functionAssertions.assertFunction(functionNamespace + "sketch_theta_summary(CAST(NULL as VARBINARY)).estimate",
                DOUBLE,
                null);
    }

    @Test
    public void testSummarySingle()
    {
        Union union = Union.builder().buildUnion();
        union.update(1);
        summaryMatches(toVarbinarySql(union.getResult().toByteArray()));
    }

    @Test
    public void testSummaryMany()
    {
        Union union = Union.builder().buildUnion();
        int size = 100;
        for (int i = 0; i < size; i++) {
            union.update(i);
        }
        summaryMatches(toVarbinarySql(union.getResult().toByteArray()));
    }

    private void summaryMatches(String sketchBytes)
    {
        MaterializedResult result = nativeQueryRunner.execute(session, format("SELECT " + functionNamespace + "sketch_theta_summary(CAST(X'%s' as VARBINARY))", sketchBytes));
        List<?> nativeSummaryRow = (List<?>) result.getOnlyValue();
        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_summary(CAST(X'%s' as VARBINARY)).estimate",
                        sketchBytes),
                DOUBLE,
                nativeSummaryRow.get(0));
        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_summary(CAST(X'%s' as VARBINARY)).theta",
                        sketchBytes),
                DOUBLE,
                nativeSummaryRow.get(1));
        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_summary(CAST(X'%s' as VARBINARY)).upper_bound_std",
                        sketchBytes),
                DOUBLE,
                nativeSummaryRow.get(2));
        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_summary(CAST(X'%s' as VARBINARY)).lower_bound_std",
                        sketchBytes),
                DOUBLE,
                nativeSummaryRow.get(3));
        functionAssertions.assertFunction(
                format(functionNamespace + "sketch_theta_summary(CAST(X'%s' as VARBINARY)).retained_entries",
                        sketchBytes),
                INTEGER,
                nativeSummaryRow.get(4));
    }

    private static String toVarbinarySql(byte[] data)
    {
        return new SqlVarbinary(data).toString().replaceAll("\\s+", " ");
    }
}
