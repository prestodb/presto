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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.QuantileTreeType;
import io.airlift.slice.Slice;

import java.util.Collections;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * Merge a set of QuantileTree sketches together to produce a QuantileTree of the set union.
 * This aggregator essentially reduces the trees into a single QuantileTree object, using
 * QuantileTree.merge(). There is no noise added in this aggregator, but any noise from the
 * existing trees will add in the merging process. The merged sketch is equivalent to
 * having produced a single sketch (though possibly with a different privacy budget, which
 * is reflected in the rho property of the final merged sketch).
 */
@AggregationFunction(value = "merge")
public final class MergeQuantileTreeAggregation
{
    private MergeQuantileTreeAggregation() {}

    @InputFunction
    public static void input(@AggregationState QuantileTreeState state, @SqlType(QuantileTreeType.NAME) Slice treeSlice)
    {
        QuantileTree tree;
        try {
            tree = QuantileTree.deserialize(treeSlice);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        if (state.getQuantileTree() == null) {
            // If nothing in state, set this as the QuantileTree in state.
            state.setQuantileTree(tree);
            state.addMemoryUsage(tree.estimatedSizeInBytes()); // all (compatible) trees will be of the same size
            // The remaining parameters are unused.
            state.setEpsilon(0);
            state.setDelta(0);
            state.setProbabilities(Collections.singletonList(0.0));
        }
        else {
            // If there is a tree in state, merge them together (throw errors gracefully if not compatible).
            try {
                state.getQuantileTree().merge(tree);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
        }
    }

    @CombineFunction
    public static void combineStates(@AggregationState QuantileTreeState state, @AggregationState QuantileTreeState otherState)
    {
        QuantileTreeAggregationUtils.combineStates(state, otherState);
    }

    @OutputFunction(QuantileTreeType.NAME)
    public static void evaluateFinal(@AggregationState QuantileTreeState state, BlockBuilder out)
    {
        if (state.getQuantileTree() == null) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, state.getQuantileTree().serialize());
        }
    }
}
