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
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.util.array.ByteBigArray;
import com.google.common.base.Optional;

import static com.facebook.presto.type.Types.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class BooleanMaxAggregation
        extends SimpleAggregationFunction
{
    public static final BooleanMaxAggregation BOOLEAN_MAX = new BooleanMaxAggregation();

    public BooleanMaxAggregation()
    {
        super(BOOLEAN, BOOLEAN, BOOLEAN);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        checkArgument(confidence == 1.0, "max does not support approximate queries");
        return new BooleanMinGroupedAccumulator(valueChannel);
    }

    public static class BooleanMinGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private static final byte NULL_VALUE = 0;
        private static final byte TRUE_VALUE = 1;
        private static final byte FALSE_VALUE = -1;

        private final ByteBigArray maxValues;

        public BooleanMinGroupedAccumulator(int valueChannel)
        {
            // Min/max are not effected by distinct, so ignore it.
            super(valueChannel, BOOLEAN, BOOLEAN, Optional.<Integer>absent(), Optional.<Integer>absent());
            this.maxValues = new ByteBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return maxValues.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            maxValues.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                // skip null values
                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    // if value is true, update the max to true
                    if (values.getBoolean()) {
                        maxValues.set(groupId, TRUE_VALUE);
                    }
                    else {
                        // if the current value is null, set the max to false
                        if (maxValues.get(groupId) == NULL_VALUE) {
                            maxValues.set(groupId, FALSE_VALUE);
                        }
                    }
                }
            }
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            byte value = maxValues.get((long) groupId);
            if (value == NULL_VALUE) {
                output.appendNull();
            }
            else {
                output.append(value == TRUE_VALUE);
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        checkArgument(confidence == 1.0, "max does not support approximate queries");
        return new BooleanMaxAccumulator(valueChannel);
    }

    public static class BooleanMaxAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private boolean max;

        public BooleanMaxAccumulator(int valueChannel)
        {
            // Min/max are not effected by distinct, so ignore it.
            super(valueChannel, BOOLEAN, BOOLEAN, Optional.<Integer>absent(), Optional.<Integer>absent());
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull()) {
                    notNull = true;

                    // if value is true, update the max to true
                    if (values.getBoolean()) {
                        max = true;
                    }
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.append(max);
            }
            else {
                out.appendNull();
            }
        }
    }
}
