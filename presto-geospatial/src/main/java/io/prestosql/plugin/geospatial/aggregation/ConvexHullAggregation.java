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
package io.prestosql.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.prestosql.geospatial.GeometryType;
import io.prestosql.geospatial.serde.GeometrySerde;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;

import java.util.Set;

import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY;
import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
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
        OGCGeometry geometry = GeometrySerde.deserialize(input);
        if (state.getGeometry() == null) {
            state.setGeometry(geometry.convexHull());
        }
        else if (!geometry.isEmpty()) {
            state.setGeometry(state.getGeometry().union(geometry).convexHull());
        }
    }

    @CombineFunction
    public static void combine(@AggregationState GeometryState state,
            @AggregationState GeometryState otherState)
    {
        if (state.getGeometry() == null) {
            state.setGeometry(otherState.getGeometry());
        }
        else if (otherState.getGeometry() != null && !otherState.getGeometry().isEmpty()) {
            state.setGeometry(state.getGeometry().union(otherState.getGeometry()).convexHull());
        }
    }

    @OutputFunction(GEOMETRY_TYPE_NAME)
    public static void output(@AggregationState GeometryState state, BlockBuilder out)
    {
        if (state.getGeometry() == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeSlice(out, GeometrySerde.serialize(state.getGeometry()));
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
