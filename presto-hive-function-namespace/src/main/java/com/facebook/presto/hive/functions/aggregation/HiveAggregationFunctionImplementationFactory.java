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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.functions.type.BlockInputDecoder;
import com.facebook.presto.hive.functions.type.BlockInputDecoders;
import com.facebook.presto.hive.functions.type.ObjectEncoders;
import com.facebook.presto.spi.function.ExternalAggregationFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.hive.functions.aggregation.HiveAccumulatorMethodHandles.getCombineFunction;
import static com.facebook.presto.hive.functions.aggregation.HiveAccumulatorMethodHandles.getInputFunction;
import static com.facebook.presto.hive.functions.aggregation.HiveAccumulatorMethodHandles.getOutputFunction;
import static java.util.Objects.requireNonNull;

public class HiveAggregationFunctionImplementationFactory
{
    private final Signature signature;
    private final List<Type> inputTypes;
    private final Type intermediateType;
    private final Type outputType;
    private final Supplier<GenericUDAFEvaluator> partialEvaluatorSupplier;
    private final Supplier<GenericUDAFEvaluator> finalEvaluatorSupplier;
    private final ObjectInspector[] inputInspectors;
    private final ObjectInspector intermediateInspector;
    private final ObjectInspector outputInspector;

    public HiveAggregationFunctionImplementationFactory(
            Signature signature,
            List<Type> inputTypes,
            Type intermediateType,
            Type outputType,
            Supplier<GenericUDAFEvaluator> partialEvaluatorSupplier,
            Supplier<GenericUDAFEvaluator> finalEvaluatorSupplier,
            ObjectInspector[] inputInspectors,
            ObjectInspector intermediateInspector,
            ObjectInspector outputInspector)
    {
        this.signature = requireNonNull(signature);
        this.inputTypes = requireNonNull(inputTypes);
        this.intermediateType = requireNonNull(intermediateType);
        this.outputType = requireNonNull(outputType);
        this.partialEvaluatorSupplier = partialEvaluatorSupplier;
        this.finalEvaluatorSupplier = finalEvaluatorSupplier;
        this.inputInspectors = requireNonNull(inputInspectors);
        this.intermediateInspector = requireNonNull(intermediateInspector);
        this.outputInspector = requireNonNull(outputInspector);
    }

    public ExternalAggregationFunctionImplementation create()
    {
        HiveAggregationFunctionDescription metadata = new HiveAggregationFunctionDescription(
                signature.getName(),
                inputTypes,
                ImmutableList.of(intermediateType),
                outputType,
                true,
                false);

        HiveAccumulatorInvoker invocationContext = new HiveAccumulatorInvoker(
                partialEvaluatorSupplier,
                finalEvaluatorSupplier,
                ObjectEncoders.createEncoder(outputType, outputInspector),
                outputType);

        List<BlockInputDecoder> inputDecoders = Streams.zip(
                inputTypes.stream(),
                Stream.of(inputInspectors),
                (type, inspector) -> BlockInputDecoders.createBlockInputDecoder(inspector, type))
                .collect(Collectors.toList());

        HiveAccumulatorFunctions methods = new HiveAccumulatorFunctions(
                getInputFunction(invocationContext, inputDecoders),
                getCombineFunction(invocationContext),
                getOutputFunction(invocationContext));

        HiveAccumulatorStateDescription stateMetadata = new HiveAccumulatorStateDescription(
                HiveAccumulatorState.class,
                new HiveAccumulatorStateSerializer(
                        partialEvaluatorSupplier,
                        finalEvaluatorSupplier,
                        intermediateType,
                        intermediateInspector),
                new HiveAccumulatorStateFactory(invocationContext::newAggregationBuffer));

        return new HiveAggregationFunctionImplementation(metadata, methods, stateMetadata);
    }
}
