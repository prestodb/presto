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

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.google.common.base.Preconditions.checkArgument;

public class TestDoubleRegrR2Aggregation
        extends AbstractTestDoubleRegrAggregationFunction
{
    @Override
    protected String getFunctionName()
    {
        return "regr_r2";
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length <= 0) {
            return null;
        }
        else if (length == 1) {
            return (double) 0;
        }
        else {
            SimpleRegression regression = new SimpleRegression();
            for (int i = start; i < start + length; i++) {
                regression.addData(i + 2, i);
            }
            return (double) regression.getRSquare();
        }
    }

    @Test
    public void testTwoSpecialCase()
    {
        // when m2x = 0, result is null
        Double[] y = new Double[] {1.0, 1.0, 1.0, 1.0, 1.0};
        Double[] x = new Double[] {1.0, 1.0, 1.0, 1.0, 1.0};
        testAggregation(null, createDoublesBlock(y), createDoublesBlock(x));

        // when m2x != 0 and m2y = 0, result is 1.0
        x = new Double[] {1.0, 2.0, 3.0, 4.0, 5.0};
        testAggregation(1.0, createDoublesBlock(y), createDoublesBlock(x));
    }

    @Override
    protected void testNonTrivialAggregation(Double[] y, Double[] x)
    {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
        double expected = (double) regression.getRSquare();
        checkArgument(Double.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createDoublesBlock(y), createDoublesBlock(x));
    }
}
