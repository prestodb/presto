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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public class TestFilterStatsCalculatorHistograms
        extends AbstractTestFilterStatsCalculator
{
    public TestFilterStatsCalculatorHistograms()
    {
        super(true);
    }

    /**
     * We override this test because the original logic utilizes heuristics in cases where stats
     * don't exist and infinite bound exists. We choose to use slightly different heuristics
     * for these cases when histograms are enabled.
     * <br>
     * See {@link StatisticRange#overlapPercentWith(StatisticRange)}
     */
    @Test
    public void testBetweenOperatorFilterLeftOpen()
    {
        assertExpression("leftOpen BETWEEN DOUBLE '-10' AND 10e0")
                .outputRowsCount(56.25)
                .variableStats(new VariableReferenceExpression(Optional.empty(), "leftOpen", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));
    }

    /**
     * We override this test because the original logic utilizes heuristics in cases where stats
     * don't exist and infinite bound exists. We choose to use slightly different heuristics
     * for these cases when histograms are enabled.
     * <br>
     * See {@link StatisticRange#overlapPercentWith(StatisticRange)}
     */
    @Test
    public void testBetweenOperatorFilterRightOpen()
    {
        // Left side open, cut on open side
        // Right side open, cut on open side
        assertExpression("rightOpen BETWEEN DOUBLE '-10' AND 10e0")
                .outputRowsCount(56.25)
                .variableStats(new VariableReferenceExpression(Optional.empty(), "rightOpen", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));
    }
}
