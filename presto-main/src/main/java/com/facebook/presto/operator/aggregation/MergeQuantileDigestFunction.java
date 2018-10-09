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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.QuantileDigestState;
import com.facebook.presto.operator.aggregation.state.QuantileDigestStateFactory;
import com.facebook.presto.operator.aggregation.state.QuantileDigestStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.type.QuantileDigestType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.stats.QuantileDigest;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.MoreMath.nearlyEqual;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

@AggregationFunction("merge")
public final class MergeQuantileDigestFunction
        extends SqlAggregationFunction
{
    public static final MergeQuantileDigestFunction MERGE = new MergeQuantileDigestFunction();
    public static final String NAME = "merge";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MergeQuantileDigestFunction.class, "input", Type.class, QuantileDigestState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MergeQuantileDigestFunction.class, "combine", QuantileDigestState.class, QuantileDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MergeQuantileDigestFunction.class, "output", QuantileDigestStateSerializer.class, QuantileDigestState.class, BlockBuilder.class);
    private static final double COMPARISON_EPSILON = 1E-6;

    public MergeQuantileDigestFunction()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("T")),
                ImmutableList.of(),
                parseTypeSignature("qdigest(T)"),
                ImmutableList.of(parseTypeSignature("qdigest(T)")));
    }

    @Override
    public String getDescription()
    {
        return "Merges the input quantile digests into a single quantile digest";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type valueType = boundVariables.getTypeVariable("T");
        QuantileDigestType outputType = (QuantileDigestType) typeManager.getParameterizedType(StandardTypes.QDIGEST,
                ImmutableList.of(TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(valueType, outputType);
    }

    private static InternalAggregationFunction generateAggregation(Type valueType, QuantileDigestType type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregationFunction.class.getClassLoader());
        QuantileDigestStateSerializer stateSerializer = new QuantileDigestStateSerializer(valueType);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), ImmutableList.of(type.getTypeSignature())),
                createInputParameterMetadata(type),
                INPUT_FUNCTION.bindTo(type),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                    QuantileDigestState.class,
                    stateSerializer,
                    new QuantileDigestStateFactory())),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, ImmutableList.of(type), ImmutableList.of(intermediateType), type, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type valueType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, valueType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    @InputFunction
    public static void input(Type type, QuantileDigestState state, Block value, int index)
    {
        merge(state, new QuantileDigest(type.getSlice(value, index)));
    }

    @CombineFunction
    public static void combine(QuantileDigestState state, QuantileDigestState otherState)
    {
        merge(state, otherState.getQuantileDigest());
    }

    private static void merge(QuantileDigestState state, QuantileDigest input)
    {
        if (input == null) {
            return;
        }
        QuantileDigest previous = state.getQuantileDigest();
        if (previous == null) {
            state.setQuantileDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            checkArgument(nearlyEqual(previous.getMaxError(), input.getMaxError(), COMPARISON_EPSILON),
                    "Cannot merge qdigests with different accuracies (%s vs. %s)", state.getQuantileDigest().getMaxError(), input.getMaxError());
            checkArgument(nearlyEqual(previous.getAlpha(), input.getAlpha(), COMPARISON_EPSILON),
                    "Cannot merge qdigests with different alpha values (%s vs. %s)", state.getQuantileDigest().getAlpha(), input.getAlpha());
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    public static void output(QuantileDigestStateSerializer serializer, QuantileDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
