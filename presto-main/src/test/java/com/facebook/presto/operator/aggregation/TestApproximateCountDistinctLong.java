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

import java.util.concurrent.ThreadLocalRandom;

public class TestApproximateCountDistinctLong
        extends AbstractTestApproximateCountDistinct
{
    @Override
    public ApproximateCountDistinctAggregation getAggregationFunction()
    {
        return ApproximateCountDistinctAggregation.LONG_INSTANCE;
    }

    @Override
    public TupleInfo.Type getValueType()
    {
        return TupleInfo.Type.FIXED_INT_64;
    }

    @Override
    public Object randomValue()
    {
        return ThreadLocalRandom.current().nextLong();
    }
}
