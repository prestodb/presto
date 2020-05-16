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

import com.esri.core.geometry.Envelope;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.geospatial.KdbTreeUtils;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.geospatial.KdbTree.buildKdbTree;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeEnvelope;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.plugin.geospatial.SpatialPartitioningAggregateFunction.NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static java.lang.Math.toIntExact;

@AggregationFunction(value = NAME, decomposable = false, visibility = HIDDEN)
public class SpatialPartitioningInternalAggregateFunction
{
    private static final int MAX_SAMPLE_COUNT = 1_000_000;

    private SpatialPartitioningInternalAggregateFunction() {}

    @InputFunction
    public static void input(SpatialPartitioningState state, @SqlType(GEOMETRY_TYPE_NAME) Slice slice, @SqlType(INTEGER) long partitionCount)
    {
        Envelope envelope = deserializeEnvelope(slice);
        if (envelope.isEmpty()) {
            return;
        }

        if (state.getCount() == 0) {
            state.setPartitionCount(toIntExact(partitionCount));
            state.setSamples(new ArrayList<>());
        }

        // use reservoir sampling
        Rectangle extent = new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax());
        List<Rectangle> samples = state.getSamples();
        if (samples.size() <= MAX_SAMPLE_COUNT) {
            samples.add(extent);
        }
        else {
            long sampleIndex = ThreadLocalRandom.current().nextLong(state.getCount());
            if (sampleIndex < MAX_SAMPLE_COUNT) {
                samples.set(toIntExact(sampleIndex), extent);
            }
        }

        state.setCount(state.getCount() + 1);
    }

    @CombineFunction
    public static void combine(SpatialPartitioningState state, SpatialPartitioningState otherState)
    {
        throw new UnsupportedOperationException("spatial_partitioning must run on a single node");
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(SpatialPartitioningState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "No rows supplied to spatial partition.");
        }

        List<Rectangle> samples = state.getSamples();

        int partitionCount = state.getPartitionCount();
        int maxItemsPerNode = (samples.size() + partitionCount - 1) / partitionCount;

        VARCHAR.writeString(out, KdbTreeUtils.toJson(buildKdbTree(maxItemsPerNode, samples)));
    }
}
