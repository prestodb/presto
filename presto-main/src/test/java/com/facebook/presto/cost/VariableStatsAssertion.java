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
package com.facebook.presto.cost;

import static com.facebook.presto.cost.EstimateAssertion.assertEstimateEquals;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class VariableStatsAssertion
{
    private final VariableStatsEstimate statistics;

    private VariableStatsAssertion(VariableStatsEstimate statistics)
    {
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    public static VariableStatsAssertion assertThat(VariableStatsEstimate actual)
    {
        return new VariableStatsAssertion(actual);
    }

    public VariableStatsAssertion nullsFraction(double expected)
    {
        assertEstimateEquals(statistics.getNullsFraction(), expected, "nullsFraction mismatch");
        return this;
    }

    public VariableStatsAssertion nullsFractionUnknown()
    {
        assertTrue(isNaN(statistics.getNullsFraction()), "expected unknown nullsFraction but got " + statistics.getNullsFraction());
        return this;
    }

    public VariableStatsAssertion lowValue(double expected)
    {
        assertEstimateEquals(statistics.getLowValue(), expected, "lowValue mismatch");
        return this;
    }

    public VariableStatsAssertion lowValueUnknown()
    {
        return lowValue(NEGATIVE_INFINITY);
    }

    public VariableStatsAssertion highValue(double expected)
    {
        assertEstimateEquals(statistics.getHighValue(), expected, "highValue mismatch");
        return this;
    }

    public VariableStatsAssertion highValueUnknown()
    {
        return highValue(POSITIVE_INFINITY);
    }

    public void empty()
    {
        this.emptyRange()
                .distinctValuesCount(0)
                .nullsFraction(1);
    }

    public VariableStatsAssertion emptyRange()
    {
        assertTrue(isNaN(statistics.getLowValue()) && isNaN(statistics.getHighValue()),
                "expected empty range (NaN, NaN) but got (" + statistics.getLowValue() + ", " + statistics.getHighValue() + ") instead");
        assertEquals(statistics.getDistinctValuesCount(), 0., "expected no distinctValuesCount");
        assertEquals(statistics.getAverageRowSize(), 0., "expected 0 average row size");
        assertEquals(statistics.getNullsFraction(), 1., "expected all nulls");
        return this;
    }

    public VariableStatsAssertion unknownRange()
    {
        return lowValueUnknown()
                .highValueUnknown();
    }

    public VariableStatsAssertion distinctValuesCount(double expected)
    {
        assertEstimateEquals(statistics.getDistinctValuesCount(), expected, "distinctValuesCount mismatch");
        return this;
    }

    public VariableStatsAssertion distinctValuesCountUnknown()
    {
        assertTrue(isNaN(statistics.getDistinctValuesCount()), "expected unknown distinctValuesCount but got " + statistics.getDistinctValuesCount());
        return this;
    }

    public VariableStatsAssertion averageRowSize(double expected)
    {
        assertEstimateEquals(statistics.getAverageRowSize(), expected, "average row size mismatch");
        return this;
    }

    public VariableStatsAssertion dataSizeUnknown()
    {
        assertTrue(isNaN(statistics.getAverageRowSize()), "expected unknown dataSize but got " + statistics.getAverageRowSize());
        return this;
    }

    public VariableStatsAssertion isEqualTo(VariableStatsEstimate expected)
    {
        return nullsFraction(expected.getNullsFraction())
                .lowValue(expected.getLowValue())
                .highValue(expected.getHighValue())
                .distinctValuesCount(expected.getDistinctValuesCount())
                .averageRowSize(expected.getAverageRowSize());
    }
}
