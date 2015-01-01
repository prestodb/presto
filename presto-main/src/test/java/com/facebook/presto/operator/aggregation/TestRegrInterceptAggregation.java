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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;

import java.util.List;

public class TestRegrInterceptAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {getDoubleSequenceBlock(start, length), getDoubleSequenceBlock(start + 2, length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "regr_intercept";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.DOUBLE, StandardTypes.DOUBLE);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length <= 1) {
            return null;
        }
        double[] independentValues = constructDoublePrimitiveArray(start, length);
        double[] dependentValues = constructDoublePrimitiveArray(start + 2, length);
        Mean mean = new Mean();
        double independentMean = mean.evaluate(independentValues);
        double dependentMean = mean.evaluate(dependentValues);
        Variance variance = new Variance(false);
        Covariance covariance = new Covariance();
        double regrSlope = covariance.covariance(independentValues, dependentValues, false) / variance.evaluate(dependentValues);
        return independentMean - regrSlope * dependentMean;
    }
}
