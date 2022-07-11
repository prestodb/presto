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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.functions.type.ObjectEncoder;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

import java.util.function.Supplier;

import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.executionError;

public final class HiveAccumulatorInvoker
{
    private final Supplier<GenericUDAFEvaluator> partialEvaluatorSupplier;
    private final Supplier<GenericUDAFEvaluator> finalEvaluatorSupplier;
    private final ObjectEncoder objectEncoder;
    private final Type outputType;

    public HiveAccumulatorInvoker(
            Supplier<GenericUDAFEvaluator> partialEvaluatorSupplier,
            Supplier<GenericUDAFEvaluator> finalEvaluatorSupplier,
            ObjectEncoder objectEncoder,
            Type outputType)
    {
        this.partialEvaluatorSupplier = partialEvaluatorSupplier;
        this.finalEvaluatorSupplier = finalEvaluatorSupplier;
        this.objectEncoder = objectEncoder;
        this.outputType = outputType;
    }

    public final AggregationBuffer newAggregationBuffer()
    {
        try {
            return partialEvaluatorSupplier.get().getNewAggregationBuffer();
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }

    public final void iterate(HiveAccumulatorState state, Object... parameters)
    {
        try {
            partialEvaluatorSupplier.get().iterate(state.getAggregationBuffer(), parameters);
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }

    public final void combine(HiveAccumulatorState state, HiveAccumulatorState otherState)
    {
        try {
            Object partial = partialEvaluatorSupplier.get().terminatePartial(otherState.getAggregationBuffer());
            finalEvaluatorSupplier.get().merge(state.getAggregationBuffer(), partial);
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }

    public final void output(HiveAccumulatorState state, BlockBuilder out)
    {
        try {
            Object terminate = finalEvaluatorSupplier.get().terminate(state.getAggregationBuffer());
            if (terminate == null) {
                out.appendNull();
            }
            else {
                writeNativeValue(outputType, out, objectEncoder.encode(terminate));
            }
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }
}
