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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.type.SqlVarbinary;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Union;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.String.format;

public class TestThetaSketchFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testNullSketch()
    {
        functionAssertions.assertFunction("sketch_theta_estimate(CAST(NULL as VARBINARY))", DOUBLE, null);
    }

    @Test
    public void testEstimateEmptySketch()
    {
        Union union = Union.builder().buildUnion();
        functionAssertions.assertFunction(
                format("sketch_theta_estimate(CAST(X'%s' as VARBINARY))",
                        toVarbinarySql(union.getResult().toByteArray())),
                DOUBLE,
                0.0);
    }

    @Test
    public void testEstimateSingleValue()
    {
        Union union = Union.builder().buildUnion();
        union.update(1);
        functionAssertions.assertFunction(
                format("sketch_theta_estimate(CAST(X'%s' as VARBINARY))",
                        toVarbinarySql(union.getResult().toByteArray())),
                DOUBLE,
                1.0);
    }

    @Test
    public void testEstimateManyValues()
    {
        Union union = Union.builder().buildUnion();
        int size = 100;
        for (int i = 0; i < size; i++) {
            union.update(i);
        }
        functionAssertions.assertFunction(
                format("sketch_theta_estimate(CAST(X'%s' as VARBINARY))",
                        toVarbinarySql(union.getResult().toByteArray())),
                DOUBLE,
                (double) size);
    }

    @Test
    public void testSummaryNull()
    {
        functionAssertions.assertFunction("sketch_theta_summary(CAST(NULL as VARBINARY)).estimate",
                DOUBLE,
                null);
    }

    @Test
    public void testSummarySingle()
    {
        Union union = Union.builder().buildUnion();
        union.update(1);
        CompactSketch compactSketch = union.getResult();
        summaryMatches(compactSketch, union.getResult().toByteArray());
    }

    @Test
    public void testSummaryMany()
    {
        Union union = Union.builder().buildUnion();
        int size = 100;
        for (int i = 0; i < size; i++) {
            union.update(i);
        }
        summaryMatches(union.getResult(), union.getResult().toByteArray());
    }

    private void summaryMatches(CompactSketch expected, byte[] input)
    {
        functionAssertions.assertFunction(
                format("sketch_theta_summary(CAST(X'%s' as VARBINARY)).estimate",
                        toVarbinarySql(input)),
                DOUBLE,
                expected.getEstimate());
        functionAssertions.assertFunction(
                format("sketch_theta_summary(CAST(X'%s' as VARBINARY)).theta",
                        toVarbinarySql(input)),
                DOUBLE,
                expected.getTheta());
        functionAssertions.assertFunction(
                format("sketch_theta_summary(CAST(X'%s' as VARBINARY)).upper_bound_std",
                        toVarbinarySql(input)),
                DOUBLE,
                expected.getUpperBound(1));
        functionAssertions.assertFunction(
                format("sketch_theta_summary(CAST(X'%s' as VARBINARY)).lower_bound_std",
                        toVarbinarySql(input)),
                DOUBLE,
                expected.getLowerBound(1));
        functionAssertions.assertFunction(
                format("sketch_theta_summary(CAST(X'%s' as VARBINARY)).retained_entries",
                        toVarbinarySql(input)),
                INTEGER,
                expected.getRetainedEntries());
    }

    private static String toVarbinarySql(byte[] data)
    {
        return new SqlVarbinary(data).toString().replaceAll("\\s+", " ");
    }
}
