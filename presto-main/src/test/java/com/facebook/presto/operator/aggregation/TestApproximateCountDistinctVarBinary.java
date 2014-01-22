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

import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slices;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations.VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TestApproximateCountDistinctVarBinary
        extends AbstractTestApproximateCountDistinct
{
    @Override
    public AggregationFunction getAggregationFunction()
    {
        return VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS;
    }

    @Override
    public TupleInfo.Type getValueType()
    {
        return VARIABLE_BINARY;
    }

    @Override
    public Object randomValue()
    {
        int length = ThreadLocalRandom.current().nextInt(100);
        byte[] bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(bytes);

        return Slices.wrappedBuffer(bytes);
    }
}
