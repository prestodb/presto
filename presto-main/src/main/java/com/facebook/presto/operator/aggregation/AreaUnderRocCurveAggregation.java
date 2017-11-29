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
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class AreaUnderRocCurveAggregation
        extends SqlAggregationFunction
{
    public static final AreaUnderRocCurveAggregation AUC = new AreaUnderRocCurveAggregation();
    public static final String NAME = "area_under_roc_curve";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AreaUnderRocCurveAggregation.class, "input", MultiKeyValuePairsState.class, Block.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AreaUnderRocCurveAggregation.class, "combine", MultiKeyValuePairsState.class, MultiKeyValuePairsState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AreaUnderRocCurveAggregation.class, "output", MultiKeyValuePairsState.class, BlockBuilder.class);

    public AreaUnderRocCurveAggregation()
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
        DynamicClassLoader classLoader = new DynamicClassLoader(AreaUnderRocCurveAggregation.class.getClassLoader());

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
        int[] indices = new int[labels.length];
        for (int i = 0; i < labels.length; i++) {
            indices[i] = i;
        }
        IntArrays.quickSort(indices, new AbstractIntComparator()
        {
            @Override
            public int compare(int i, int j)
            {
                return Double.compare(scores[j], scores[i]);
            }
        });

        int truePositive = 0;
        int falsePositive = 0;

        int truePositivePrev = 0;
        int falsePositivePrev = 0;

        double area = 0.0;

        double scorePrev = Double.POSITIVE_INFINITY;

        for (int i : indices) {
            if (Double.compare(scores[i], scorePrev) != 0) {
                area += trapezoidArea(falsePositive, falsePositivePrev, truePositive, truePositivePrev);

                scorePrev = scores[i];
                falsePositivePrev = falsePositive;
                truePositivePrev = truePositive;
            }
            if (labels[i]) {
                truePositive++;
            }
            else {
                falsePositive++;
            }
        }

        // AUC for single class outcome is not defined
        if (truePositive == 0 || falsePositive == 0) {
            return Double.POSITIVE_INFINITY;
        }

        // finalize by adding a trapezoid area based on the final tp/fp counts
        area += trapezoidArea(falsePositive, falsePositivePrev, truePositive, truePositivePrev);

        return area / (truePositive * falsePositive); // scaled value in the 0-1 range
    }

    /**
     * Calculate trapezoidal area under (x1, y1)-(x2, y2)
     */
    private static double trapezoidArea(double x1, double x2, double y1, double y2)
    {
        double base = Math.abs(x1 - x2);
        double averageHeight = (y1 + y2) / 2;
        return base * averageHeight;
    }
}
