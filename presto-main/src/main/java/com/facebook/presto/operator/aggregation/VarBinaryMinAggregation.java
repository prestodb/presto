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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.util.array.ObjectBigArray;
import com.google.common.base.Optional;
import io.airlift.slice.Slice;

import static com.facebook.presto.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class VarBinaryMinAggregation
        extends SimpleAggregationFunction
{
    public static final VarBinaryMinAggregation VAR_BINARY_MIN = new VarBinaryMinAggregation();

    public VarBinaryMinAggregation()
    {
        super(VARCHAR, VARCHAR, VARCHAR);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        checkArgument(confidence == 1.0, "min does not support approximate queries");
        return new VarBinaryGroupedAccumulator(valueChannel);
    }

    public static class VarBinaryGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final ObjectBigArray<Slice> minValues;
        private long sizeOfValues;

        public VarBinaryGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, VARCHAR, VARCHAR, Optional.<Integer>absent(), Optional.<Integer>absent());
            this.minValues = new ObjectBigArray<>();
        }

        @Override
        public long getEstimatedSize()
        {
            return minValues.sizeOf() + sizeOfValues;
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            minValues.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                // skip null values
                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    Slice value = values.getSlice();
                    Slice currentValue = minValues.get(groupId);
                    if (currentValue == null || value.compareTo(currentValue) < 0) {
                        minValues.set(groupId, value);

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
            Slice value = minValues.get((long) groupId);
            if (value == null) {
                output.appendNull();
            }
            else {
                output.append(value);
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // Min/max are not effected by distinct, so ignore it.
        checkArgument(confidence == 1.0, "min does not support approximate queries");
        return new VarBinaryMinAccumulator(valueChannel);
    }

    public static class VarBinaryMinAccumulator
            extends SimpleAccumulator
    {
        private Slice min;

        public VarBinaryMinAccumulator(int valueChannel)
        {
            super(valueChannel, VARCHAR, VARCHAR, Optional.<Integer>absent(), Optional.<Integer>absent());
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull()) {
                    min = min(min, values.getSlice());
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (min != null) {
                out.append(min);
            }
            else {
                out.appendNull();
            }
        }
    }

    private static Slice min(Slice a, Slice b)
    {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) < 0 ? a : b;
    }
}
