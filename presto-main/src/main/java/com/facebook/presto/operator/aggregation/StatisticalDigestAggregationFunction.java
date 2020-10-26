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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestStateFactory;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.QDIGEST;
import static com.facebook.presto.common.type.StandardTypes.TDIGEST;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.DEFAULT_ACCURACY;
import static com.facebook.presto.operator.scalar.TDigestFunctions.DEFAULT_COMPRESSION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.insertArguments;

public abstract class StatisticalDigestAggregationFunction
        extends SqlAggregationFunction
{
    private static final long DEFAULT_WEIGHT = 1L;
    private final String name;
    private final String type;
    private final StatisticalDigestStateFactory factory;

    private static final MethodHandle T_DIGEST_INPUT_DOUBLE = methodHandle(TDigestAggregationFunction.class,
            "inputDouble",
            StatisticalDigestState.class,
            double.class,
            long.class,
            double.class);
    private static final MethodHandle QUANTILE_DIGEST_INPUT_DOUBLE = methodHandle(
            QuantileDigestAggregationFunction.class,
            "inputDouble",
            StatisticalDigestState.class,
            double.class,
            long.class,
            double.class);
    private static final MethodHandle INPUT_REAL = methodHandle(QuantileDigestAggregationFunction.class,
            "inputReal",
            StatisticalDigestState.class,
            long.class,
            long.class,
            double.class);
    private static final MethodHandle INPUT_BIGINT = methodHandle(
            QuantileDigestAggregationFunction.class,
            "inputBigint",
            StatisticalDigestState.class,
            long.class,
            long.class,
            double.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(
            StatisticalDigestAggregationFunction.class,
            "combineState",
            StatisticalDigestState.class,
            StatisticalDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(
            StatisticalDigestAggregationFunction.class,
            "evaluateFinal",
            StatisticalDigestStateSerializer.class,
            StatisticalDigestState.class,
            BlockBuilder.class);

    StatisticalDigestAggregationFunction(String name, String type, StatisticalDigestStateFactory factory, SqlFunctionVisibility visibility, TypeSignature... typeSignatures)
    {
        super(new Signature(
                        QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, name),
                        AGGREGATE,
                        ImmutableList.of(comparableTypeParameter("V")),
                        ImmutableList.of(),
                        parseTypeSignature(type + "(V)"),
                        ImmutableList.copyOf(typeSignatures),
                        false),
                visibility);
        this.name = name;
        this.type = type;
        this.factory = factory;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type valueType = boundVariables.getTypeVariable("V");
        Type outputType = functionAndTypeManager.getParameterizedType(
                type,
                ImmutableList.of(TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(name, valueType, outputType, arity);
    }

    private InternalAggregationFunction generateAggregation(String name, Type valueType, Type outputType, int arity)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(StatisticalDigestAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = getInputTypes(valueType, arity);
        StatisticalDigestStateSerializer stateSerializer = new StatisticalDigestStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputTypes),
                getInputMethodHandle(valueType, arity),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        StatisticalDigestState.class,
                        stateSerializer,
                        factory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    private static List<Type> getInputTypes(Type valueType, int arity)
    {
        switch (arity) {
            case 1:
                // weight and accuracy unspecified
                return ImmutableList.of(valueType);
            case 2:
                // weight specified, accuracy unspecified
                return ImmutableList.of(valueType, BIGINT);
            case 3:
                // weight and accuracy specified
                return ImmutableList.of(valueType, BIGINT, DOUBLE);
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported number of arguments: %s", arity));
        }
    }

    private MethodHandle getInputMethodHandle(Type valueType, int arity)
    {
        switch (type) {
            case TDIGEST:
                return getTDigestInputMethodHandle(valueType, arity);
            case QDIGEST:
                return getQuantileDigestInputMethodHandle(valueType, arity);
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s must be a statistical digest", type));
        }
    }

    private MethodHandle getTDigestInputMethodHandle(Type valueType, int arity)
    {
        final MethodHandle inputFunction;
        switch (valueType.getDisplayName()) {
            case StandardTypes.DOUBLE:
                inputFunction = T_DIGEST_INPUT_DOUBLE;
                break;
            case StandardTypes.REAL:
                throw new PrestoException(NOT_SUPPORTED, "Cannot operate on a t-digest with real numbers");
            case StandardTypes.BIGINT:
                throw new PrestoException(NOT_SUPPORTED, "Cannot operate on a t-digest with longs");
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type %s supplied", valueType.getDisplayName()));
        }
        return addArguments(inputFunction, arity, DEFAULT_COMPRESSION);
    }

    private static MethodHandle getQuantileDigestInputMethodHandle(Type valueType, int arity)
    {
        final MethodHandle inputFunction;
        switch (valueType.getDisplayName()) {
            case StandardTypes.DOUBLE:
                inputFunction = QUANTILE_DIGEST_INPUT_DOUBLE;
                break;
            case StandardTypes.REAL:
                inputFunction = INPUT_REAL;
                break;
            case StandardTypes.BIGINT:
                inputFunction = INPUT_BIGINT;
                break;
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type %s supplied", valueType.getDisplayName()));
        }
        return addArguments(inputFunction, arity, DEFAULT_ACCURACY);
    }

    private static MethodHandle addArguments(MethodHandle inputFunction, int arity, double parameter)
    {
        switch (arity) {
            case 1:
                // weight and accuracy unspecified
                return insertArguments(inputFunction, 2, DEFAULT_WEIGHT, parameter);
            case 2:
                // weight specified, accuracy unspecified
                return insertArguments(inputFunction, 3, parameter);
            case 3:
                // weight and accuracy specified
                return inputFunction;
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported number of arguments: %s", arity));
        }
    }

    private static List<ParameterMetadata> createInputParameterMetadata(List<Type> valueTypes)
    {
        return ImmutableList.<ParameterMetadata>builder()
                .add(new ParameterMetadata(STATE))
                .addAll(valueTypes.stream().map(valueType -> new ParameterMetadata(INPUT_CHANNEL, valueType)).collect(Collectors.toList()))
                .build();
    }

    public static void combineState(StatisticalDigestState state, StatisticalDigestState otherState)
    {
        StatisticalDigest input = otherState.getStatisticalDigest();
        StatisticalDigest previous = state.getStatisticalDigest();

        if (previous == null) {
            state.setStatisticalDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    public static void evaluateFinal(StatisticalDigestStateSerializer serializer, StatisticalDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
