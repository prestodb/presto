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

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.google.common.base.Preconditions.checkArgument;

public class TestRealRegrAvgxAggregation
        extends AbstractTestRealRegrAggregationFunction
{
    @Override
    protected String getFunctionName()
    {
        return "regr_avgx";
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return 0.0f;
        }

        float expected = 0.0f;
        for (int i = start; i < start + length; i++) {
            expected += (i + 2);
        }
        return expected / length;
    }

    @Override
    protected void testNonTrivialAggregation(Float[] y, Float[] x)
    {
        float expected = 0.0f;
        for (int i = 0; i < x.length; i++) {
            expected += x[i];
        }
        expected = expected / x.length;
        checkArgument(Float.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createBlockOfReals(y), createBlockOfReals(x));
    }
}
