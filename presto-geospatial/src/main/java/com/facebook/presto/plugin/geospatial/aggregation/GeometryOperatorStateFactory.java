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
package com.facebook.presto.plugin.geospatial.aggregation;

import com.esri.core.geometry.OperatorConvexHull;
import com.esri.core.geometry.OperatorUnion;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class GeometryOperatorStateFactory
        implements AccumulatorStateFactory<GeometryOperatorState>
{
    private final GeometryOperatorFactory operatorFactory;

    public GeometryOperatorStateFactory(GeometryOperatorFactory operatorFactory)
    {
        this.operatorFactory = operatorFactory;
    }

    @Override
    public GeometryOperatorState createSingleState()
    {
        return new SingleGeometryOperatorState(operatorFactory);
    }

    @Override
    public Class<? extends GeometryOperatorState> getSingleStateClass()
    {
        return SingleGeometryOperatorState.class;
    }

    @Override
    public GeometryOperatorState createGroupedState()
    {
        return new GroupedGeometryOperatorState(operatorFactory);
    }

    @Override
    public Class<? extends GeometryOperatorState> getGroupedStateClass()
    {
        return GroupedGeometryOperatorState.class;
    }

    public static final class GeometryConvexHullOperatorStateFactory
            extends GeometryOperatorStateFactory
    {
        public GeometryConvexHullOperatorStateFactory()
        {
            super((input, spatialReference) -> OperatorConvexHull.local().execute(input, true, null));
        }
    }

    public static final class GeometryUnionOperatorStateFactory
            extends GeometryOperatorStateFactory
    {
        public GeometryUnionOperatorStateFactory()
        {
            super((input, spatialReference) -> OperatorUnion.local().execute(input, spatialReference, null));
        }
    }
}
