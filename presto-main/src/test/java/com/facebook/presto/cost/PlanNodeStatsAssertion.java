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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.function.Consumer;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.cost.EstimateAssertion.assertEstimateEquals;
import static com.google.common.collect.Sets.union;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PlanNodeStatsAssertion
{
    private final PlanNodeStatsEstimate actual;

    private PlanNodeStatsAssertion(PlanNodeStatsEstimate actual)
    {
        this.actual = actual;
    }

    public static PlanNodeStatsAssertion assertThat(PlanNodeStatsEstimate actual)
    {
        return new PlanNodeStatsAssertion(actual);
    }

    public PlanNodeStatsAssertion outputRowsCount(double expected)
    {
        assertEstimateEquals(actual.getOutputRowCount(), expected, "outputRowsCount mismatch");
        return this;
    }

    public PlanNodeStatsAssertion totalSize(double expected)
    {
        assertEstimateEquals(actual.getOutputSizeInBytes(), expected, "totalSize mismatch");
        return this;
    }

    public PlanNodeStatsAssertion confident(boolean expected)
    {
        assertEquals(actual.isConfident(), expected);
        return this;
    }

    public PlanNodeStatsAssertion outputRowsCountUnknown()
    {
        assertTrue(Double.isNaN(actual.getOutputRowCount()), "expected unknown outputRowsCount but got " + actual.getOutputRowCount());
        return this;
    }

    public PlanNodeStatsAssertion variableStats(VariableReferenceExpression variable, Consumer<VariableStatsAssertion> columnAssertionConsumer)
    {
        VariableStatsAssertion columnAssertion = VariableStatsAssertion.assertThat(actual.getVariableStatistics(variable));
        columnAssertionConsumer.accept(columnAssertion);
        return this;
    }

    public PlanNodeStatsAssertion variableStatsUnknown(String symbolName)
    {
        return variableStatsUnknown(new VariableReferenceExpression(symbolName, BIGINT));
    }

    public PlanNodeStatsAssertion variableStatsUnknown(VariableReferenceExpression variable)
    {
        return variableStats(variable,
                columnStats -> columnStats
                        .lowValueUnknown()
                        .highValueUnknown()
                        .nullsFractionUnknown()
                        .distinctValuesCountUnknown());
    }

    public PlanNodeStatsAssertion variablesWithKnownStats(VariableReferenceExpression... variable)
    {
        assertEquals(actual.getVariablesWithKnownStatistics(), ImmutableSet.copyOf(variable), "variables with known stats");
        return this;
    }

    public PlanNodeStatsAssertion equalTo(PlanNodeStatsEstimate expected)
    {
        assertEstimateEquals(actual.getOutputRowCount(), expected.getOutputRowCount(), "outputRowCount mismatch");
        assertEquals(actual.isConfident(), expected.isConfident());

        for (VariableReferenceExpression variable : union(expected.getVariablesWithKnownStatistics(), actual.getVariablesWithKnownStatistics())) {
            assertVariableStatsEqual(variable, actual.getVariableStatistics(variable), expected.getVariableStatistics(variable));
        }
        return this;
    }

    private void assertVariableStatsEqual(VariableReferenceExpression variable, VariableStatsEstimate actual, VariableStatsEstimate expected)
    {
        assertEstimateEquals(actual.getNullsFraction(), expected.getNullsFraction(), "nullsFraction mismatch for %s", variable.getName());
        assertEstimateEquals(actual.getLowValue(), expected.getLowValue(), "lowValue mismatch for %s", variable.getName());
        assertEstimateEquals(actual.getHighValue(), expected.getHighValue(), "highValue mismatch for %s", variable.getName());
        assertEstimateEquals(actual.getDistinctValuesCount(), expected.getDistinctValuesCount(), "distinct values count mismatch for %s", variable.getName());
        assertEstimateEquals(actual.getAverageRowSize(), expected.getAverageRowSize(), "average row size mismatch for %s", variable.getName());
    }
}
