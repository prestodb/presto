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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SecureRandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.PrestoException;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class QuantileTreeAggregationUtils
{
    // These parameters were chosen in N5336227.
    public static final int DEFAULT_BIN_COUNT = 65_536; // 2^16
    public static final int DEFAULT_BRANCHING_FACTOR = 4;
    public static final int DEFAULT_SKETCH_DEPTH = 3;
    public static final int DEFAULT_SKETCH_WIDTH = 400;

    private QuantileTreeAggregationUtils() {}

    public static void initializeState(QuantileTreeState state, Block probabilities, double epsilon, double delta,
            double lower, double upper, int binCount, int branchingFactor, int sketchDepth, int sketchWidth)
    {
        if (epsilon <= 0 || delta <= 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "epsilon and delta must be positive");
        }

        List<Double> probabilitiesArray = new ArrayList<>();
        for (int i = 0; i < probabilities.getPositionCount(); i++) {
            if (probabilities.isNull(i)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "probability cannot be null");
            }

            double probability = DOUBLE.getDouble(probabilities, i);
            if (probability < 0 || probability > 1) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "cumulative probability must be between 0 and 1");
            }
            probabilitiesArray.add(probability);
        }

        if (probabilitiesArray.isEmpty()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "must pass in at least 1 probability");
        }

        QuantileTree quantileTree = state.getQuantileTree();
        if (quantileTree == null) {
            try {
                quantileTree = new QuantileTree(lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
            state.setProbabilities(probabilitiesArray);
            state.setEpsilon(epsilon);
            state.setDelta(delta);
            state.setQuantileTree(quantileTree);
            state.addMemoryUsage(quantileTree.estimatedSizeInBytes());
        }
    }

    public static void inputValue(QuantileTreeState state, double value, Block probabilities, double epsilon, double delta,
            double lower, double upper, int binCount, int branchingFactor, int sketchDepth, int sketchWidth)
    {
        initializeState(state, probabilities, epsilon, delta, lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth);
        state.getQuantileTree().add(value);
    }

    public static void inputValue(QuantileTreeState state, double value, double probability, double epsilon, double delta,
            double lower, double upper, int binCount, int branchingFactor, int sketchDepth, int sketchWidth)
    {
        BlockBuilder probabilityBlockBuilder = DOUBLE.createFixedSizeBlockBuilder(1);
        DOUBLE.writeDouble(probabilityBlockBuilder, probability);
        Block singleProbabilityBlock = probabilityBlockBuilder.build();
        inputValue(state, value, singleProbabilityBlock, epsilon, delta, lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth);
    }

    public static void combineStates(QuantileTreeState state, QuantileTreeState otherState)
    {
        QuantileTree tree = state.getQuantileTree();
        QuantileTree otherTree = otherState.getQuantileTree();
        if (tree == null) {
            // If no tree in state, take values from otherState.
            state.setQuantileTree(otherTree);
            state.setEpsilon(otherState.getEpsilon());
            state.setDelta(otherState.getDelta());
            state.setProbabilities(otherState.getProbabilities());
            state.addMemoryUsage(otherTree.estimatedSizeInBytes());
        }
        else {
            try {
                tree.merge(otherTree);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
        }
    }

    /**
     * Enables privacy in the QuantileTree stored in state, if any
     * <p>
     * Note: If epsilon is infinite or delta >= 1, this is a no-op, since those parameters yield no privacy.
     */
    public static void enablePrivacy(QuantileTreeState state)
    {
        if (state.getQuantileTree() == null) {
            return;
        }
        double rho = QuantileTree.getRhoForEpsilonDelta(state.getEpsilon(), state.getDelta());
        if (rho != QuantileTree.NON_PRIVATE_RHO) {
            state.getQuantileTree().enablePrivacy(rho, new SecureRandomizationStrategy());
        }
    }

    public static void writeSketch(QuantileTreeState state, BlockBuilder out)
    {
        if (state.getQuantileTree() == null) {
            out.appendNull();
        }
        else {
            QuantileTreeAggregationUtils.enablePrivacy(state);
            VARBINARY.writeSlice(out, state.getQuantileTree().serialize());
        }
    }
}
