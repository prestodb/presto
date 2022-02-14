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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.functions.type.BlockInputDecoder;
import com.facebook.presto.hive.functions.type.BlockInputDecoders;
import com.facebook.presto.hive.functions.type.ObjectEncoder;
import com.facebook.presto.hive.functions.type.ObjectEncoders;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.function.Supplier;

import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.executionError;

public class HiveAccumulatorStateSerializer
        implements AccumulatorStateSerializer<HiveAccumulatorState>
{
    private final Supplier<GenericUDAFEvaluator> partialEvaluatorSupplier;
    private final Supplier<GenericUDAFEvaluator> finalEvaluatorSupplier;
    private final Type type;
    private final ObjectEncoder encoder;
    private final BlockInputDecoder decoder;

    public HiveAccumulatorStateSerializer(
            Supplier<GenericUDAFEvaluator> partialEvaluatorSupplier,
            Supplier<GenericUDAFEvaluator> finalEvaluatorSupplier,
            Type type,
            ObjectInspector inspector)
    {
        this.partialEvaluatorSupplier = partialEvaluatorSupplier;
        this.finalEvaluatorSupplier = finalEvaluatorSupplier;
        this.type = type;
        this.encoder = ObjectEncoders.createEncoder(type, inspector);
        this.decoder = BlockInputDecoders.createBlockInputDecoder(inspector, type);
    }

    @Override
    public Type getSerializedType()
    {
        return type;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void serialize(HiveAccumulatorState state, BlockBuilder out)
    {
        try {
            AggregationBuffer buf = state.getAggregationBuffer();
            Object partial = partialEvaluatorSupplier.get().terminatePartial(buf);
            writeNativeValue(type, out, encoder.encode(partial));
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void deserialize(Block block, int index, HiveAccumulatorState state)
    {
        try {
            AggregationBuffer buf = finalEvaluatorSupplier.get().getNewAggregationBuffer();
            Object partial = decoder.decode(block, index);
            finalEvaluatorSupplier.get().merge(buf, partial);
            state.setAggregationBuffer(buf);
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }
}
