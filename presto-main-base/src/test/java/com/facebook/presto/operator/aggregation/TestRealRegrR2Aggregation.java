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

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.google.common.base.Preconditions.checkArgument;

public class TestRealRegrR2Aggregation
        extends AbstractTestRealRegrAggregationFunction
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
            return (float) 0;
        }
        else {
            SimpleRegression regression = new SimpleRegression();
            for (int i = start; i < start + length; i++) {
                regression.addData(i + 2, i);
            }
            return (float) regression.getRSquare();
        }
    }

    @Test
    public void testTwoSpecialCase()
    {
        // when m2x = 0, result is null
        Float[] y = new Float[] {1.0f, 1.0f, 1.0f, 1.0f, 1.0f};
        Float[] x = new Float[] {1.0f, 1.0f, 1.0f, 1.0f, 1.0f};
        testAggregation(null, createBlockOfReals(y), createBlockOfReals(x));

        // when m2x != 0 and m2y = 0, result is 1.0
        x = new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        testAggregation(1.0f, createBlockOfReals(y), createBlockOfReals(x));
    }

    @Override
    protected void testNonTrivialAggregation(Float[] y, Float[] x)
    {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
        float expected = (float) regression.getRSquare();
        checkArgument(Float.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createBlockOfReals(y), createBlockOfReals(x));
    }
}
