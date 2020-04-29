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

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.geospatial.serde.EsriGeometrySerde;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;

import java.util.Set;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

/**
 * Aggregate form of ST_ConvexHull, which takes a set of geometries and computes the convex hull
 * of all the geometries in the set. The output is a single geometry.
 */
@Description("Returns a geometry that is the convex hull of all the geometries in the set.")
@AggregationFunction("convex_hull_agg")
public class ConvexHullAggregation
{
    private static final Joiner OR_JOINER = Joiner.on(" or ");

    private ConvexHullAggregation() {}

    @InputFunction
    public static void input(@AggregationState GeometryState state,
            @SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
        if (state.getGeometry() == null) {
            state.setGeometry(geometry.convexHull(), 0);
        }
        else if (!geometry.isEmpty()) {
            long previousMemorySize = state.getGeometry().estimateMemorySize();
            state.setGeometry(state.getGeometry().union(geometry).convexHull(), previousMemorySize);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState GeometryState state,
            @AggregationState GeometryState otherState)
    {
        if (state.getGeometry() == null) {
            state.setGeometry(otherState.getGeometry(), 0);
        }
        else if (otherState.getGeometry() != null && !otherState.getGeometry().isEmpty()) {
            long previousMemorySize = state.getGeometry().estimateMemorySize();
            state.setGeometry(state.getGeometry().union(otherState.getGeometry()).convexHull(), previousMemorySize);
        }
    }

    @OutputFunction(GEOMETRY_TYPE_NAME)
    public static void output(@AggregationState GeometryState state, BlockBuilder out)
    {
        if (state.getGeometry() == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeSlice(out, EsriGeometrySerde.serialize(state.getGeometry()));
        }
    }

    private static void validateType(String function, OGCGeometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!validTypes.contains(type)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s only applies to %s. Input type is: %s", function, OR_JOINER.join(validTypes), type));
        }
    }
}
