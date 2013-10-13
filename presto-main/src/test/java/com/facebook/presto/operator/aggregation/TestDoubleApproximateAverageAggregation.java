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

import com.facebook.presto.type.Type;

import java.util.List;

import static com.facebook.presto.operator.aggregation.ApproximateAverageAggregations.DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION;
import static com.facebook.presto.type.Types.DOUBLE;

public class TestDoubleApproximateAverageAggregation
        extends AbstractTestApproximateAggregationFunction
{
    @Override
    public AggregationFunction getFunction()
    {
        return DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION;
    }

    @Override
    protected Type getType()
    {
        return DOUBLE;
    }

    @Override
    protected Double getExpectedValue(List<Number> values)
    {
        int length = 0;
        double sum = 0;

        for (Number value : values) {
            if (value == null) {
                continue;
            }
            length++;
            sum += value.doubleValue();
        }

        if (length == 0) {
            return null;
        }

        return sum / length;
    }
}
