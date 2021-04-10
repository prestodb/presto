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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestStateFactory;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestStateSerializer;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;

public abstract class MergeStatisticalDigestFunction
        extends SqlAggregationFunction
{
    public final String name;
    public final String type;
    public final StatisticalDigestStateFactory factory;

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(
            MergeStatisticalDigestFunction.class,
            "combine",
            StatisticalDigestState.class,
            StatisticalDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(
            MergeStatisticalDigestFunction.class,
            "output",
            StatisticalDigestStateSerializer.class,
            StatisticalDigestState.class,
            BlockBuilder.class);

    MergeStatisticalDigestFunction(String name, String type, StatisticalDigestStateFactory factory, SqlFunctionVisibility visibility)
    {
        super(new Signature(
                        QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, name),
                        AGGREGATE,
                        ImmutableList.of(comparableTypeParameter("T")),
                        ImmutableList.of(),
                        parseTypeSignature(type + "(T)"),
                        ImmutableList.of(parseTypeSignature(type + "(T)")),
                        false),
                visibility);
        this.name = name;
        this.type = type;
        this.factory = factory;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type valueType = boundVariables.getTypeVariable("T");
        Type outputType = functionAndTypeManager.getParameterizedType(type,
                ImmutableList.of(TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(outputType);
    }

    private InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregationFunction.class.getClassLoader());
        StatisticalDigestStateSerializer stateSerializer = new StatisticalDigestStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, type.getTypeSignature(), ImmutableList.of(type.getTypeSignature())),
                createInputParameterMetadata(type),
                getInputFunction().bindTo(type),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                    StatisticalDigestState.class,
                    stateSerializer,
                    factory)),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, ImmutableList.of(type), ImmutableList.of(intermediateType), type, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type valueType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, valueType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    protected abstract MethodHandle getInputFunction();

    public static void combine(StatisticalDigestState state, StatisticalDigestState otherState)
    {
        merge(state, otherState.getStatisticalDigest());
    }

    protected static void merge(StatisticalDigestState state, StatisticalDigest input)
    {
        if (input == null) {
            return;
        }
        StatisticalDigest previous = state.getStatisticalDigest();
        if (previous == null) {
            state.setStatisticalDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input.getDigest());
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    public static void output(StatisticalDigestStateSerializer serializer, StatisticalDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
