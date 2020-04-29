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
package com.facebook.presto.plugin.geospatial;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.plugin.geospatial.SpatialPartitioningAggregateFunction.NAME;

@AggregationFunction(value = NAME, decomposable = false)
public class SpatialPartitioningAggregateFunction
{
    public static final String NAME = "spatial_partitioning";

    private SpatialPartitioningAggregateFunction() {}

    @InputFunction
    public static void input(SpatialPartitioningState state, @SqlType(GEOMETRY_TYPE_NAME) Slice slice)
    {
        throw new UnsupportedOperationException("spatial_partitioning(geometry, samplingPercentage) aggregate function should be re-written into spatial_partitioning(geometry, samplingPercentage, partitionCount)");
    }

    @CombineFunction
    public static void combine(SpatialPartitioningState state, SpatialPartitioningState otherState)
    {
        throw new UnsupportedOperationException("spatial_partitioning(geometry, samplingPercentage) aggregate function should be re-written into spatial_partitioning(geometry, samplingPercentage, partitionCount)");
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(SpatialPartitioningState state, BlockBuilder out)
    {
        throw new UnsupportedOperationException("spatial_partitioning(geometry, samplingPercentage) aggregate function should be re-written into spatial_partitioning(geometry, samplingPercentage, partitionCount)");
    }
}
