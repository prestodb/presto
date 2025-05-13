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
package com.facebook.presto.operator.aggregation.sketch.kll;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.TypeParameter;

public class KllSketchStateFactory
        implements AccumulatorStateFactory<KllSketchAggregationState>
{
    private final Type type;

    public KllSketchStateFactory(@TypeParameter("T") Type type)
    {
        this.type = type;
    }

    @Override
    public KllSketchAggregationState createSingleState()
    {
        return new KllSketchAggregationState.Single(type);
    }

    @Override
    public Class<? extends KllSketchAggregationState> getSingleStateClass()
    {
        return KllSketchAggregationState.Single.class;
    }

    @Override
    public KllSketchAggregationState createGroupedState()
    {
        return new KllSketchAggregationState.Grouped(type);
    }

    @Override
    public Class<? extends KllSketchAggregationState> getGroupedStateClass()
    {
        return KllSketchAggregationState.Grouped.class;
    }
}
