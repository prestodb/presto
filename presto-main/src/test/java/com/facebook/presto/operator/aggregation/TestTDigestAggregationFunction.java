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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.tdigest.TDigest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.TDIGEST;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.TestMergeTDigestFunction.TDIGEST_EQUALITY;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.sort;

public class TestTDigestAggregationFunction
        extends TestStatisticalDigestAggregationFunction
{
    private static final double STANDARD_COMPRESSION_FACTOR = 100;

    protected double getParameter()
    {
        return STANDARD_COMPRESSION_FACTOR;
    }

    @Override
    protected InternalAggregationFunction getAggregationFunction(Type... type)
    {
        FunctionAndTypeManager functionAndTypeManager = METADATA.getFunctionAndTypeManager();
        return functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("tdigest_agg", fromTypes(type)));
    }

    @Override
    protected void testAggregationDoubles(InternalAggregationFunction function, Page page, double error, double... inputs)
    {
        assertAggregation(function,
                TDIGEST_EQUALITY,
                "test multiple positions",
                page,
                getExpectedValueDoubles(STANDARD_COMPRESSION_FACTOR, inputs));

        // test scalars
        List<Double> rows = Arrays.stream(inputs).sorted().boxed().collect(Collectors.toList());

        SqlVarbinary returned = (SqlVarbinary) AggregationTestUtils.aggregation(function, page);
        assertPercentileWithinError(TDIGEST, StandardTypes.DOUBLE, returned, 0.01, rows, 0.1, 0.5, 0.9, 0.99);
        assertValueWithinError(StandardTypes.DOUBLE, returned, STANDARD_COMPRESSION_FACTOR, rows, 0.1, 0.5, 0.9, 0.99);
    }

    @Override
    protected Object getExpectedValueDoubles(double compression, double... values)
    {
        if (values.length == 0) {
            return null;
        }
        TDigest tDigest = createTDigest(compression);
        Arrays.stream(values).forEach(tDigest::add);
        return new SqlVarbinary(tDigest.serialize().getBytes());
    }

    private void assertValueWithinError(String type, SqlVarbinary binary, double error, List<? extends Number> rows, double... percentiles)
    {
        if (rows.isEmpty()) {
            // Nothing to assert except that the tdigest is empty
            return;
        }

        // Test each quantile individually (value_at_quantile)
        for (double percentile : percentiles) {
            assertValueWithinError(type, binary, error, rows, percentile);
        }

        // Test all the quantiles (values_at_quantiles)
        assertValuesWithinError(type, binary, error, rows, percentiles);
    }

    private void assertValueWithinError(String type, SqlVarbinary binary, double error, List<? extends Number> rows, double percentile)
    {
        Number lowerBound = getLowerBoundQuantile(percentile, error);
        Number upperBound = getUpperBoundQuantile(percentile, error);

        // Check that the chosen quantile is within the upper and lower bound of the error
        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) >= %s",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        sortNumberList(rows).get((int) (rows.size() * percentile)).doubleValue(),
                        lowerBound),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("quantile_at_value(CAST(X'%s' AS tdigest(%s)), %s) <= %s",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        sortNumberList(rows).get((int) (rows.size() * percentile)).doubleValue(),
                        upperBound),
                BOOLEAN,
                true);
    }

    private void assertValuesWithinError(String type, SqlVarbinary binary, double error, List<? extends Number> rows, double[] percentiles)
    {
        List<Double> boxedPercentiles = Arrays.stream(percentiles).sorted().boxed().collect(toImmutableList());
        List<Double> boxedValues = boxedPercentiles.stream().map(percentile -> sortNumberList(rows).get((int) (rows.size() * percentile)).doubleValue()).collect(toImmutableList());
        List<Number> lowerBounds = boxedPercentiles.stream().map(percentile -> getLowerBoundQuantile(percentile, error)).collect(toImmutableList());
        List<Number> upperBounds = boxedPercentiles.stream().map(percentile -> getUpperBoundQuantile(percentile, error)).collect(toImmutableList());

        // Ensure that the lower bound of each item in the distribution is not greater than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(quantiles_at_values(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, lowerbound) -> value >= lowerbound)",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        ARRAY_JOINER.join(boxedValues),
                        ARRAY_JOINER.join(lowerBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));

        // Ensure that the upper bound of each item in the distribution is not less than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(quantiles_at_values(CAST(X'%s' AS tdigest(%s)), ARRAY[%s]), ARRAY[%s], (value, upperbound) -> value <= upperbound)",
                        binary.toString().replaceAll("\\s+", " "),
                        type,
                        ARRAY_JOINER.join(boxedValues),
                        ARRAY_JOINER.join(upperBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));
    }

    private Number getLowerBoundQuantile(double quantile, double error)
    {
        return Math.max(0, quantile - error);
    }

    private Number getUpperBoundQuantile(double quantile, double error)
    {
        return Math.min(1, quantile + error);
    }

    private List<? extends Number> sortNumberList(List<? extends Number> list)
    {
        List<? extends Number> sorted = new ArrayList<>(list);
        sort(sorted, new Comparator<Number>() {
            @Override
            public int compare(Number o1, Number o2)
            {
                Double d1 = (o1 == null) ? Double.POSITIVE_INFINITY : o1.doubleValue();
                Double d2 = (o2 == null) ? Double.POSITIVE_INFINITY : o2.doubleValue();
                return d1.compareTo(d2);
            }
        });
        return sorted;
    }
}
