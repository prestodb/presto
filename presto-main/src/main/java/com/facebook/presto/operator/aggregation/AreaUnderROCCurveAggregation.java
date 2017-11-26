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
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.MultiKeyValuePairStateSerializer;
import com.facebook.presto.operator.aggregation.state.MultiKeyValuePairsState;
import com.facebook.presto.operator.aggregation.state.MultiKeyValuePairsStateFactory;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class AreaUnderROCCurveAggregation
        extends SqlAggregationFunction
{
    public static final AreaUnderROCCurveAggregation AUC = new AreaUnderROCCurveAggregation();
    public static final String NAME = "area_under_roc_curve";
    private static final MethodHandle
            INPUT_FUNCTION = methodHandle(AreaUnderROCCurveAggregation.class, "input", MultiKeyValuePairsState.class, Block.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AreaUnderROCCurveAggregation.class, "combine", MultiKeyValuePairsState.class, MultiKeyValuePairsState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AreaUnderROCCurveAggregation.class, "output", MultiKeyValuePairsState.class, BlockBuilder.class);

    protected AreaUnderROCCurveAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                DOUBLE.getTypeSignature(),
                ImmutableList.of(BOOLEAN.getTypeSignature(), DOUBLE.getTypeSignature()));
    }

    @Override
    public String getDescription()
    {
        return "Returns AUC";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return generateAggregation();
    }

    protected InternalAggregationFunction generateAggregation()
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AreaUnderROCCurveAggregation.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(BOOLEAN, DOUBLE);

        MultiKeyValuePairStateSerializer stateSerializer = new MultiKeyValuePairStateSerializer(BOOLEAN, DOUBLE);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, DOUBLE.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(),
                INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                MultiKeyValuePairsState.class,
                stateSerializer,
                new MultiKeyValuePairsStateFactory(BOOLEAN, DOUBLE),
                DOUBLE);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, DOUBLE, true, factory);
    }

    private static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata()
    {
        return ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE),
                new AggregationMetadata.ParameterMetadata(BLOCK_INPUT_CHANNEL, BOOLEAN),
                new AggregationMetadata.ParameterMetadata(BLOCK_INPUT_CHANNEL, DOUBLE),
                new AggregationMetadata.ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(MultiKeyValuePairsState state, Block label, Block score, int position)
    {
        MultiKeyValuePairs pairs = state.get();
        if (pairs == null) {
            pairs = new MultiKeyValuePairs(state.getKeyType(), state.getValueType());
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        pairs.add(label, score, position, position);
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    public static void combine(MultiKeyValuePairsState state, MultiKeyValuePairsState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            Block labelsBlock = otherState.get().getKeys();
            Block scoresBlock = otherState.get().getValues();
            MultiKeyValuePairs pairs = state.get();
            long startSize = pairs.estimatedInMemorySize();
            for (int i = 0; i < labelsBlock.getPositionCount(); i++) {
                pairs.add(labelsBlock, scoresBlock, i, i);
            }
            state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void output(MultiKeyValuePairsState state, BlockBuilder out)
    {
        MultiKeyValuePairs pairs = state.get();
        if (pairs == null) {
            out.appendNull();
        }
        else {
            Block labelsBlock = pairs.getKeys();
            Block scoresBlock = pairs.getValues();
            boolean[] labels = new boolean[labelsBlock.getPositionCount()];
            double[] scores = new double[labelsBlock.getPositionCount()];
            for (int i = 0; i < labelsBlock.getPositionCount(); i++) {
                labels[i] = BOOLEAN.getBoolean(labelsBlock, i);
                scores[i] = DOUBLE.getDouble(scoresBlock, i);
            }
            double auc = computeAUC(labels, scores);
            if (Double.isFinite(auc)) {
                DOUBLE.writeDouble(out, auc);
            }
            else {
                out.appendNull();
            }
        }
    }

    /**
     * Calculate Area Under the ROC Curve using trapezoidal rule
     * See Algorithm 4 in a paper "ROC Graphs: Notes and Practical Considerations
     * for Data Mining Researchers" for detail:
     * http://www.hpl.hp.com/techreports/2003/HPL-2003-4.pdf
     */
    private static double computeAUC(boolean[] labels, double[] scores)
    {
        // labels sorted descending by scores
        Integer[] sortedIndices = IntStream.range(0, labels.length).boxed().sorted((i, j) -> (int) Math.signum(scores[j] - scores[i])).toArray(Integer[]::new);

        int tp = 0;
        int fp = 0;

        int tpPrev = 0;
        int fpPrev = 0;

        double area = 0.0;

        double scorePrev = Double.POSITIVE_INFINITY;

        for (int i : sortedIndices) {
            if (scores[i] != scorePrev) {
                area += trapezoidArea(fp, fpPrev, tp, tpPrev);

                scorePrev = scores[i];
                fpPrev = fp;
                tpPrev = tp;
            }
            if (labels[i]) {
                tp++;
            }
            else {
                fp++;
            }
        }

        // AUC for single class outcome is not defined
        if (tp == 0 || fp == 0) {
            return Double.POSITIVE_INFINITY;
        }

        // finalize by adding a trapezoid area based on the final tp/fp counts
        area += trapezoidArea(fp, fpPrev, tp, tpPrev);

        return area / (tp * fp); // scaled value in the 0-1 range
    }

    /**
     * Calculate trapezoidal area under (x1, y1)-(x2, y2)
     */
    private static double trapezoidArea(double x1, double x2, double y1, double y2)
    {
        double base = Math.abs(x1 - x2);
        double height = (y1 + y2) / 2.0;
        return base * height;
    }
}
