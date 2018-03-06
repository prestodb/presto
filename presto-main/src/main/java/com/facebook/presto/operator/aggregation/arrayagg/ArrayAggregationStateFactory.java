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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation.LEGACY;
import static com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation.NEW;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.lang.String.format;

public class ArrayAggregationStateFactory
        implements AccumulatorStateFactory<ArrayAggregationState>
{
    private final Type type;
    private final ArrayAggGroupImplementation mode;

    public ArrayAggregationStateFactory(Type type, ArrayAggGroupImplementation mode)
    {
        this.type = type;
        this.mode = mode;
    }

    @Override
    public ArrayAggregationState createSingleState()
    {
        return new SingleArrayAggregationState(type);
    }

    @Override
    public Class<? extends ArrayAggregationState> getSingleStateClass()
    {
        return SingleArrayAggregationState.class;
    }

    @Override
    public ArrayAggregationState createGroupedState()
    {
        if (mode == NEW) {
            return new GroupArrayAggregationState(type);
        }

        if (mode == LEGACY) {
            return new LegacyArrayAggregationGroupState(type);
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("Unexpected group enum type %s", mode));
    }

    @Override
    public Class<? extends ArrayAggregationState> getGroupedStateClass()
    {
        return GroupArrayAggregationState.class;
    }
}
