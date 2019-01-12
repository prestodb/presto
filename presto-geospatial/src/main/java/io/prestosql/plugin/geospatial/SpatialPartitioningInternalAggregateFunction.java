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
package io.prestosql.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import io.airlift.slice.Slice;
import io.prestosql.geospatial.KdbTreeUtils;
import io.prestosql.geospatial.Rectangle;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.prestosql.geospatial.KdbTree.buildKdbTree;
import static io.prestosql.geospatial.serde.GeometrySerde.deserializeEnvelope;
import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static io.prestosql.plugin.geospatial.SpatialPartitioningAggregateFunction.NAME;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;

@AggregationFunction(value = NAME, decomposable = false, hidden = true)
public class SpatialPartitioningInternalAggregateFunction
{
    private static final int MAX_SAMPLE_COUNT = 1_000_000;

    private SpatialPartitioningInternalAggregateFunction() {}

    @InputFunction
    public static void input(SpatialPartitioningState state, @SqlType(GEOMETRY_TYPE_NAME) Slice slice, @SqlType(INTEGER) long partitionCount)
    {
        Envelope envelope = deserializeEnvelope(slice);
        if (envelope == null) {
            return;
        }

        Rectangle extent = new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax());

        if (state.getCount() == 0) {
            state.setPartitionCount(toIntExact(partitionCount));
            state.setExtent(extent);
            state.setSamples(new ArrayList<>());
        }
        else {
            state.setExtent(state.getExtent().merge(extent));
        }

        // use reservoir sampling
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
            out.appendNull();
            return;
        }

        List<Rectangle> samples = state.getSamples();

        int partitionCount = state.getPartitionCount();
        int maxItemsPerNode = (samples.size() + partitionCount - 1) / partitionCount;
        Rectangle envelope = state.getExtent();

        // Add a small buffer on the right and upper sides
        Rectangle paddedExtent = new Rectangle(envelope.getXMin(), envelope.getYMin(), Math.nextUp(envelope.getXMax()), Math.nextUp(envelope.getYMax()));

        VARCHAR.writeString(out, KdbTreeUtils.toJson(buildKdbTree(maxItemsPerNode, paddedExtent, samples)));
    }
}
