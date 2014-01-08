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
import com.facebook.presto.util.array.ObjectBigArray;
import com.google.common.base.Optional;
import io.airlift.slice.Slice;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkState;

public class VarBinaryMaxAggregation
        extends SimpleAggregationFunction
{
    public static final VarBinaryMaxAggregation VAR_BINARY_MAX = new VarBinaryMaxAggregation();

    public VarBinaryMaxAggregation()
    {
        super(SINGLE_VARBINARY, SINGLE_VARBINARY, VARIABLE_BINARY);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        return new VarBinaryMaxGroupedAccumulator(valueChannel);
    }

    public static class VarBinaryMaxGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final ObjectBigArray<Slice> maxValues;
        private long sizeOfValues;

        public VarBinaryMaxGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY, Optional.<Integer>absent(), Optional.<Integer>absent());

            this.maxValues = new ObjectBigArray<>();
        }

        @Override
        public long getEstimatedSize()
        {
            return maxValues.sizeOf() + sizeOfValues;
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

                    Slice value = values.getSlice();
                    Slice currentValue = maxValues.get(groupId);
                    if (currentValue == null || value.compareTo(currentValue) > 0) {
                        maxValues.set(groupId, value);

                        // update size
                        if (currentValue != null) {
                            sizeOfValues -= currentValue.length();
                        }
                        sizeOfValues += value.length();
                    }
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            Slice value = maxValues.get((long) groupId);
            if (value == null) {
                output.appendNull();
            }
            else {
                output.append(value);
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        return new VarBinaryMaxAccumulator(valueChannel);
    }

    public static class VarBinaryMaxAccumulator
            extends SimpleAccumulator
    {
        private Slice max;

        public VarBinaryMaxAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY, Optional.<Integer>absent(), Optional.<Integer>absent());
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull()) {
                    max = max(max, values.getSlice());
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (max != null) {
                out.append(max);
            }
            else {
                out.appendNull();
            }
        }
    }

    private static Slice max(Slice a, Slice b)
    {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) > 0 ? a : b;
    }
}
