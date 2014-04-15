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

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.ObjectBigArray;
import com.google.common.base.Optional;
import io.airlift.stats.cardinality.HyperLogLog;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class ApproximateSetAggregation
        extends SimpleAggregationFunction
{
    private static final int NUMBER_OF_BUCKETS = 4096;
    private final Type parameterType;

    public ApproximateSetAggregation(Type parameterType)
    {
        super(HYPER_LOG_LOG, HYPER_LOG_LOG, parameterType);

        checkArgument(parameterType.equals(BIGINT) || parameterType.equals(DOUBLE) || parameterType.equals(VARCHAR),
                "Expected parameter type to be BIGINT, DOUBLE, or VARCHAR, but was %s",
                parameterType);

        this.parameterType = parameterType;
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "confidence level must be 1.0");
        return new ApproximateSetGroupedAccumulator(parameterType, valueChannel, maskChannel, sampleWeightChannel);
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "confidence level must be 1.0");
        return new ApproximateSetAccumulator(parameterType, valueChannel, maskChannel);
    }

    static class ApproximateSetAccumulator
            extends SimpleAccumulator
    {
        private final Type parameterType;

        private HyperLogLog estimator;

        public ApproximateSetAccumulator(Type parameterType, int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, HyperLogLogType.HYPER_LOG_LOG, HyperLogLogType.HYPER_LOG_LOG, maskChannel, Optional.<Integer>absent());
            this.parameterType = parameterType;
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    if (estimator == null) {
                        estimator = HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
                    }

                    add(values, parameterType, estimator);
                }
            }
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                if (!intermediates.isNull()) {
                    HyperLogLog instance = HyperLogLog.newInstance(intermediates.getSlice());

                    if (estimator == null) {
                        estimator = instance;
                    }
                    else {
                        estimator.mergeWith(instance);
                    }
                }
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            evaluateFinal(out);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (estimator == null) {
                out.appendNull();
            }
            else {
                out.append(estimator.serialize());
            }
        }
    }

    private static void add(BlockCursor cursor, Type parameterType, HyperLogLog estimator)
    {
        if (parameterType.equals(BIGINT)) {
            estimator.add(cursor.getLong());
        }
        else if (parameterType.equals(DOUBLE)) {
            estimator.add(Double.doubleToLongBits(cursor.getDouble()));
        }
        else if (parameterType.equals(VARCHAR)) {
            estimator.add(cursor.getSlice());
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT, DOUBLE, or VARCHAR");
        }
    }

    static class ApproximateSetGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final ObjectBigArray<HyperLogLog> estimators = new ObjectBigArray<>();
        private final Type parameterType;
        private long sizeOfValues;

        public ApproximateSetGroupedAccumulator(Type parameterType, int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, HYPER_LOG_LOG, HYPER_LOG_LOG, maskChannel, sampleWeightChannel);
            this.parameterType = parameterType;
        }

        @Override
        public long getEstimatedSize()
        {
            return sizeOfValues;
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            estimators.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());

                // skip null values
                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    HyperLogLog hll = estimators.get(groupId);
                    if (hll == null) {
                        hll = HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
                        estimators.set(groupId, hll);
                        sizeOfValues += hll.estimatedInMemorySize();
                    }

                    sizeOfValues -= hll.estimatedInMemorySize();
                    add(values, parameterType, hll);
                    sizeOfValues += hll.estimatedInMemorySize();
                }
            }
            checkState(!values.advanceNextPosition(), "group id and value blocks have different number of entries");
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            estimators.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediates = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());

                // skip null values
                if (!intermediates.isNull()) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    HyperLogLog input = HyperLogLog.newInstance(intermediates.getSlice());

                    HyperLogLog previous = estimators.get(groupId);
                    if (previous == null) {
                        estimators.set(groupId, input);
                        sizeOfValues += input.estimatedInMemorySize();
                    }
                    else {
                        sizeOfValues -= previous.estimatedInMemorySize();
                        previous.mergeWith(input);
                        sizeOfValues += previous.estimatedInMemorySize();
                    }
                }
            }
            checkState(!intermediates.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            evaluateFinal(groupId, output);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            HyperLogLog estimator = estimators.get(groupId);
            if (estimator == null) {
                output.appendNull();
            }
            else {
                output.append(estimator.serialize());
            }
        }
    }
}
