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
import com.facebook.presto.geospatial.serde.GeometrySerde;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;

/**
 * Aggregate form of ST_Union which takes a set of geometries and unions them into a single geometry, resulting in no intersecting
 * regions.  The output may be a multi-geometry, a single geometry or a geometry collection.
 */
@Description("Returns a geometry that represents the point set union of the input geometries.")
@AggregationFunction("geometry_union_agg")
public class GeometryUnionAgg
{
    private GeometryUnionAgg() {}

    @InputFunction
    public static void input(@AggregationState GeometryOperatorState.GeometryUnionOperatorState state, @SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = GeometrySerde.deserialize(input);
        state.add(geometry);
    }

    @CombineFunction
    public static void combine(@AggregationState GeometryOperatorState.GeometryUnionOperatorState state, @AggregationState GeometryOperatorState.GeometryUnionOperatorState otherState)
    {
        OGCGeometry geometry = otherState.getGeometry();
        state.add(geometry);
    }

    @OutputFunction(GEOMETRY_TYPE_NAME)
    public static void output(@AggregationState GeometryOperatorState.GeometryUnionOperatorState state, BlockBuilder out)
    {
        OGCGeometry geometry = state.getGeometry();
        if (geometry == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeSlice(out, GeometrySerde.serialize(geometry));
        }
    }
}
