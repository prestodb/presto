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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;

public interface VariableWidthAggregationFunction<T>
        extends AggregationFunction
{
    /**
     * @return the initial value for the aggregation
     */
    T initialize();

    /**
     * Add all of the values in the specified block to the aggregation.
     *
     * @param positionCount number of positions in this page
     * @param blocks the blocks containing values for the aggregation; empty for no-arg aggregations
     * @param fields
     */
    T addInput(int positionCount, Block[] blocks, int[] fields, T currentValue);

    /**
     * Add the current value of the specified cursor to the aggregation.
     *
     * @param cursors the values to add to the aggregation; empty for no-arg aggregations
     * @param fields
     */
    T addInput(BlockCursor[] cursors, int[] fields, T currentValue);

    /**
     * Add the intermediate value at specified cursor to the aggregation.
     * The intermediate value is a value produced by the <code>evaluateIntermediate</code> function.
     *
     * @param cursors the values to add to the aggregation; empty for no-arg aggregations
     * @param fields
     */
    T addIntermediate(BlockCursor[] cursors, int[] fields, T currentValue);

    /**
     * Converts the current value to an intermediate value and adds it to the specified output.
     */
    void evaluateIntermediate(T currentValue, BlockBuilder output);

    /**
     * Converts the current value to a final value and adds it to the specified output.
     */
    void evaluateFinal(T currentValue, BlockBuilder output);

    long estimateSizeInBytes(T value);
}
