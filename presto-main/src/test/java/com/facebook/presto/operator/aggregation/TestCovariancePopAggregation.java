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

import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.constructDoublePrimitiveArray;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.createDoubleSequenceBlock;

public class TestCovariancePopAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[]{createDoubleSequenceBlock(start, length), createDoubleSequenceBlock(start + 5, length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "covar_pop";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.DOUBLE, StandardTypes.DOUBLE);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length <= 0) {
            return null;
        }
        else if (length == 1) {
            return 0.;
        }
        Covariance covariance = new Covariance();
        return covariance.covariance(constructDoublePrimitiveArray(start + 5, length), constructDoublePrimitiveArray(start, length), false);
    }
}
