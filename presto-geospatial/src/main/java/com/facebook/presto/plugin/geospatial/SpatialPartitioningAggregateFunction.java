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
import com.facebook.presto.geospatial.KDBTreeBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Suppliers;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static com.facebook.presto.geospatial.serde.GeometrySerde.deserializeEnvelope;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

@AggregationFunction(value = "spatial_partitioning", isOrderSensitive = true)
public class SpatialPartitioningAggregateFunction
{
    private static final Supplier<ObjectMapper> OBJECT_MAPPER = Suppliers.memoize(() -> new ObjectMapperProvider().get().registerModule(new SimpleModule()));

    private SpatialPartitioningAggregateFunction() {}

    @InputFunction
    public static void input(SpatialPartitioningState state, @SqlType(GEOMETRY_TYPE_NAME) Slice slice, @SqlType(DOUBLE) double percent)
    {
        Envelope envelope = deserializeEnvelope(slice);
        if (envelope == null) {
            return;
        }

        if (state.getCount() == 0) {
            state.setEnvelope(envelope);
        }
        else {
            state.getEnvelope().merge(envelope);
        }

        if (ThreadLocalRandom.current().nextDouble() < percent * 0.01) {
            if (state.getSamples() == null) {
                List<Envelope> samples = new ArrayList<>();
                samples.add(envelope);
                state.setSamples(samples);
            }
            else {
                state.getSamples().add(envelope);
            }
        }

        state.setCount(state.getCount() + 1);
    }

    @CombineFunction
    public static void combine(SpatialPartitioningState state, SpatialPartitioningState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }

        if (state.getCount() == 0) {
            state.setEnvelope(otherState.getEnvelope());
        }
        else {
            state.getEnvelope().merge(otherState.getEnvelope());
        }

        if (otherState.getSamples() != null) {
            if (state.getSamples() == null) {
                state.setSamples(otherState.getSamples());
            }
            else {
                state.getSamples().addAll(otherState.getSamples());
            }
        }

        state.setCount(state.getCount() + otherState.getCount());
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(SpatialPartitioningState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            List<Envelope> samples = state.getSamples();

            int targetNodeCount = 333;
            int maxItemsPerNode = (int) (Math.ceil(samples.size() * 1.0 / targetNodeCount));
            KDBTreeBuilder kdbTreeBuilder = new KDBTreeBuilder(maxItemsPerNode, targetNodeCount, state.getEnvelope());
            for (Envelope sample : samples) {
                kdbTreeBuilder.insert(sample);
            }

            try {
                String json = OBJECT_MAPPER.get().writer().writeValueAsString(kdbTreeBuilder.build());
                VARCHAR.writeString(out, json);
            }
            catch (JsonProcessingException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
    }
}
