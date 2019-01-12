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
package io.prestosql.cost;

import static io.prestosql.cost.EstimateAssertion.assertEstimateEquals;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SymbolStatsAssertion
{
    private final SymbolStatsEstimate statistics;

    private SymbolStatsAssertion(SymbolStatsEstimate statistics)
    {
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    public static SymbolStatsAssertion assertThat(SymbolStatsEstimate actual)
    {
        return new SymbolStatsAssertion(actual);
    }

    public SymbolStatsAssertion nullsFraction(double expected)
    {
        assertEstimateEquals(statistics.getNullsFraction(), expected, "nullsFraction mismatch");
        return this;
    }

    public SymbolStatsAssertion nullsFractionUnknown()
    {
        assertTrue(isNaN(statistics.getNullsFraction()), "expected unknown nullsFraction but got " + statistics.getNullsFraction());
        return this;
    }

    public SymbolStatsAssertion lowValue(double expected)
    {
        assertEstimateEquals(statistics.getLowValue(), expected, "lowValue mismatch");
        return this;
    }

    public SymbolStatsAssertion lowValueUnknown()
    {
        return lowValue(NEGATIVE_INFINITY);
    }

    public SymbolStatsAssertion highValue(double expected)
    {
        assertEstimateEquals(statistics.getHighValue(), expected, "highValue mismatch");
        return this;
    }

    public SymbolStatsAssertion highValueUnknown()
    {
        return highValue(POSITIVE_INFINITY);
    }

    public void empty()
    {
        this.emptyRange()
                .distinctValuesCount(0)
                .nullsFraction(1);
    }

    public SymbolStatsAssertion emptyRange()
    {
        assertTrue(isNaN(statistics.getLowValue()) && isNaN(statistics.getHighValue()),
                "expected empty range (NaN, NaN) but got (" + statistics.getLowValue() + ", " + statistics.getHighValue() + ") instead");
        assertEquals(statistics.getDistinctValuesCount(), 0., "expected no distinctValuesCount");
        assertEquals(statistics.getAverageRowSize(), 0., "expected 0 average row size");
        assertEquals(statistics.getNullsFraction(), 1., "expected all nulls");
        return this;
    }

    public SymbolStatsAssertion unknownRange()
    {
        return lowValueUnknown()
                .highValueUnknown();
    }

    public SymbolStatsAssertion distinctValuesCount(double expected)
    {
        assertEstimateEquals(statistics.getDistinctValuesCount(), expected, "distinctValuesCount mismatch");
        return this;
    }

    public SymbolStatsAssertion distinctValuesCountUnknown()
    {
        assertTrue(isNaN(statistics.getDistinctValuesCount()), "expected unknown distinctValuesCount but got " + statistics.getDistinctValuesCount());
        return this;
    }

    public SymbolStatsAssertion averageRowSize(double expected)
    {
        assertEstimateEquals(statistics.getAverageRowSize(), expected, "average row size mismatch");
        return this;
    }

    public SymbolStatsAssertion dataSizeUnknown()
    {
        assertTrue(isNaN(statistics.getAverageRowSize()), "expected unknown dataSize but got " + statistics.getAverageRowSize());
        return this;
    }

    public SymbolStatsAssertion isEqualTo(SymbolStatsEstimate expected)
    {
        return nullsFraction(expected.getNullsFraction())
                .lowValue(expected.getLowValue())
                .highValue(expected.getHighValue())
                .distinctValuesCount(expected.getDistinctValuesCount())
                .averageRowSize(expected.getAverageRowSize());
    }
}
