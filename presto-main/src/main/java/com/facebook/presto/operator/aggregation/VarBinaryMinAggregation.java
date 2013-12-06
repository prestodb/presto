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
import io.airlift.slice.Slice;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkState;

public class VarBinaryMinAggregation
        extends SimpleAggregationFunction
{
    public static final VarBinaryMinAggregation VAR_BINARY_MIN = new VarBinaryMinAggregation();

    public VarBinaryMinAggregation()
    {
        super(SINGLE_VARBINARY, SINGLE_VARBINARY, VARIABLE_BINARY);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        return new VarBinaryGroupedAccumulator(valueChannel);
    }

    public static class VarBinaryGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final ObjectBigArray<Slice> minValues;
        private long sizeOfValues;

        public VarBinaryGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY);
            this.minValues = new ObjectBigArray<>();
        }

        @Override
        public long getEstimatedSize()
        {
            return minValues.sizeOf() + sizeOfValues;
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
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
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new VarBinaryMinAccumulator(valueChannel);
    }

    public static class VarBinaryMinAccumulator
            extends SimpleAccumulator
    {
        private Slice min;

        public VarBinaryMinAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY);
        }

        @Override
        protected void processInput(Block block)
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
