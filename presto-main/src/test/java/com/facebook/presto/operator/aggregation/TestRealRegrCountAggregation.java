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

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.google.common.base.Preconditions.checkArgument;

public class TestRealRegrCountAggregation
        extends AbstractTestRealRegrAggregationFunction
{
    @Override
    protected String getFunctionName()
    {
        return "regr_count";
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length <= 1) {
            return (float) length;
        }
        else {
            SimpleRegression regression = new SimpleRegression();
            for (int i = start; i < start + length; i++) {
                regression.addData(i + 2, i);
            }
            return (float) regression.getN();
        }
    }

    @Override
    protected void testNonTrivialAggregation(Float[] y, Float[] x)
    {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
        float expected = (float) regression.getN();
        checkArgument(Float.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createBlockOfReals(y), createBlockOfReals(x));
    }
}
