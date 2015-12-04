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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.*;
import com.facebook.presto.operator.aggregation.state.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.type.*;
import com.facebook.presto.type.*;
import com.facebook.presto.util.array.*;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.*;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalLimit;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.*;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TruncatedHistogram
        extends SqlAggregationFunction
{
    public static final TruncatedHistogram TRUNCATED_HISTOGRAM = new TruncatedHistogram();
    public static final String NAME = "truncated_histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(TruncatedHistogram.class, "output", State.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(TruncatedHistogram.class, "input", Type.class, State.class, long.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(TruncatedHistogram.class, "combine", State.class, State.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;

    public static class TypedTruncatedHistogram
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedTruncatedHistogram.class).instanceSize();

        private static final float FILL_RATIO = 0.9f;
        private static final float COMPACT_RATIO = 0.5f;
        private static final long FOUR_MEGABYTES = new DataSize(4, MEGABYTE).toBytes();

        private int maxFill;
        private int mask;
        private final int compactLimit;

        private final Type type;

        private BlockBuilder values;
        private IntBigArray hashPositions;
        private final LongBigArray counts;

        public TypedTruncatedHistogram(Type type, int expectedSize, int compactLimit)
        {
            this.type = type;
            this.compactLimit = compactLimit;

            checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

            int hashSize = arraySize(expectedSize, FILL_RATIO);

            maxFill = calculateMaxFill(hashSize);
            mask = hashSize - 1;
            values = this.type.createBlockBuilder(new BlockBuilderStatus(), hashSize, expectedValueSize(type, hashSize));
            hashPositions = new IntBigArray(-1);
            hashPositions.ensureCapacity(hashSize);
            counts = new LongBigArray();
            counts.ensureCapacity(hashSize);
        }

        public long getEstimatedSize()
        {
            return INSTANCE_SIZE +
                    values.getRetainedSizeInBytes() +
                    counts.sizeOf() +
                    hashPositions.sizeOf();
        }

        private Block getValues()
        {
            return values.build();
        }

        private LongBigArray getCounts()
        {
            return counts;
        }

        public void addAll(TypedTruncatedHistogram other)
        {
            Block otherValues = other.getValues();
            LongBigArray otherCounts = other.getCounts();
            for (int i = 0; i < otherValues.getPositionCount(); i++) {
                long count = otherCounts.get(i);
                if (count > 0) {
                    add(i, otherValues, count);
                }
            }
        }

        public void add(int position, Block block, long count)
        {
            int hashPosition = getHashPosition(com.facebook.presto.type.TypeUtils.hashPosition(type, block, position), mask);

            // look for an empty slot or a slot containing this key
            while (true) {
                if (hashPositions.get(hashPosition) == -1) {
                    break;
                }

                if (type.equalTo(block, position, values, hashPositions.get(hashPosition))) {
                    counts.add(hashPositions.get(hashPosition), count);
                    return;
                }

                // increment position and mask to handle wrap around
                hashPosition = (hashPosition + 1) & mask;
            }

            addNewGroup(hashPosition, position, block, count);
        }

        private void addNewGroup(int hashPosition, int position, Block block, long count)
        {
            hashPositions.set(hashPosition, values.getPositionCount());
            counts.set(values.getPositionCount(), count);
            type.appendTo(block, position, values);

            compactIfNeeded();

            rehashIfNeeded();

            if (getEstimatedSize() > FOUR_MEGABYTES) {
                throw exceededLocalLimit(new DataSize(4, MEGABYTE));
            }
        }

        private void rehashIfNeeded() {
            // increase capacity, if necessary
            if (values.getPositionCount() >= maxFill) {
                int size = maxFill * 2;
                int hashSize = arraySize(size + 1, FILL_RATIO);
                mask = hashSize - 1;
                rehash();
            }
        }

        private void rehash()
        {
            int hashSize = mask + 1;
            IntBigArray newHashPositions = new IntBigArray(-1);
            newHashPositions.ensureCapacity(hashSize);

            for (int i = 0; i < values.getPositionCount(); i++) {
                // find an empty slot for the address
                int hashPosition = getHashPosition(com.facebook.presto.type.TypeUtils.hashPosition(type, values, i), mask);
                while (newHashPositions.get(hashPosition) != -1) {
                    hashPosition = (hashPosition + 1) & mask;
                }

                // record the mapping
                newHashPositions.set(hashPosition, i);
            }

            maxFill = calculateMaxFill(hashSize);
            hashPositions = newHashPositions;

            this.counts.ensureCapacity(maxFill);
        }

        /**
         * Call {@link #compact()} if over {@link #COMPACT_RATIO} of the value/count pairs exceeds {@link #compactLimit}.
         */
        private void compactIfNeeded() {
            if (values.getPositionCount() * COMPACT_RATIO > compactLimit) {
                compact();
            }
        }

        /**
         * Ensure there are no more than {@link #compactLimit} value/counts pairs.
         * This works by evicting the smallest counts and overflowing the counts to the smallest retained counts, increasing
         * them all to the same value (rounding up).
         * The end result is that the estimated counts are an upper bound to the true counts.
         * In addition, the over-estimation is less than the minimum retained count.
         * This is because every overflow increases the minimum retained count by the most.
         */
        private void compact() {
            if (values.getPositionCount() <= compactLimit) {
                return;
            }

            int truncatedNum = values.getPositionCount() - compactLimit;
            assert 0 < truncatedNum;
            assert truncatedNum < values.getPositionCount();

            int hashSize = mask + 1;
            BlockBuilder newValues = type.createBlockBuilder(new BlockBuilderStatus(), hashSize, expectedValueSize(type, hashSize));

            // Sort the counts so we know which counts to truncate.
            long[] countsSorted = new long[values.getPositionCount()];
            for (int i = 0; i < values.getPositionCount(); i++) {
                countsSorted[i] = counts.get(i);
            }
            Arrays.sort(countsSorted);

            // Redistribute truncated counts approximately to the overflowed counts:
            // If countsCopy = [1, 1, 2, 3, 10, 15, 20] and we need to truncate the 2 counts [1, 1]...
            long truncatedSum = 0;
            // Number equal to max truncated. 2 in this case. Used to break ties.
            long maxTruncated = countsSorted[truncatedNum - 1];
            int maxTruncatedCount = 0;
            for (int i = 0; i < truncatedNum; ++i) {
                truncatedSum += countsSorted[i];
                if (countsSorted[i] == maxTruncated) {
                    ++maxTruncatedCount;
                }
            }

            // Overflow [1, 1] into the start of [2, 3, 10, 15, 20] by increasing the smallest prefix of the array to a
            // constant, maintaining sorted order.
            // For the example, overflow until the 10, producing [4, 4, 10, 15, 20].
            // This is where overflowSum = sum([1, 1, 2, 3]) <= sum([4, 4]) <= 10 * 2.
            int overflowUntil = truncatedNum;
            long overflowSum = truncatedSum;
            while (overflowUntil < countsSorted.length &&
                    countsSorted[overflowUntil] * (overflowUntil - truncatedNum) < overflowSum) {
                overflowSum += countsSorted[overflowUntil++];
            }
            int overflowNum = overflowUntil - truncatedNum;
            long overflowConstant = overflowNum == 0 ? 0 : (overflowSum + (overflowNum - 1)) / overflowNum;

            // Copy the value/count pairs over, skipping truncated counts and "overflowing" other counts.
            for (int oldPosition = 0; oldPosition < values.getPositionCount(); ++oldPosition) {
                long oldCount = counts.get(oldPosition);

                // Skip the truncated values
                if (oldCount < maxTruncated
                        // If there's a tie, break it by first-come-first-serve
                        || (oldCount == maxTruncated && maxTruncatedCount-- > 0)) {
                    continue;
                }

                final int newPosition = newValues.getPositionCount();
                assert newPosition <= oldPosition;
                counts.set(newPosition, Math.max(overflowConstant, oldCount));
                values.writePositionTo(oldPosition, newValues);
                newValues.closeEntry();
            }

            values = newValues;
            assert values.getPositionCount() == compactLimit;

            rehash();
        }

        private static int getHashPosition(long rawHash, int mask)
        {
            return ((int) murmurHash3(rawHash)) & mask;
        }

        private static int calculateMaxFill(int hashSize)
        {
            checkArgument(hashSize > 0, "hashSize must greater than 0");
            int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
            if (maxFill == hashSize) {
                maxFill--;
            }
            checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
            return maxFill;
        }

        public void writeMapTo(BlockBuilder out) {
            compact();

            Block valuesBlock = values.build();
            BlockBuilder blockBuilder = out.beginBlockEntry();
            for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
                type.appendTo(valuesBlock, i, blockBuilder);
                BIGINT.writeLong(blockBuilder, counts.get(i));
            }
            out.closeEntry();
        }

        public void serialize(BlockBuilder out) {
            BlockBuilder blockBuilder = out.beginBlockEntry();

            BIGINT.writeLong(blockBuilder, compactLimit);
            writeMapTo(blockBuilder);

            out.closeEntry();
        }

        public static TypedTruncatedHistogram deserialize(Type type, Block block) {
            requireNonNull(block, "block is null");
            int compactLimit = Ints.checkedCast(BIGINT.getLong(block, 0));
            final TypedTruncatedHistogram histogram = new TypedTruncatedHistogram(type, EXPECTED_SIZE_FOR_HASHING, compactLimit);

            Block mapBlock = new MapType(type, BIGINT).getObject(block, 1);
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                histogram.add(i, mapBlock, BIGINT.getLong(mapBlock, i + 1));
            }

            return histogram;
        }
    }

    @AccumulatorStateMetadata(stateSerializerClass = State.Serializer.class, stateFactoryClass = State.Factory.class)
    public interface State
            extends AccumulatorState
    {
        void set(TypedTruncatedHistogram histogram);

        TypedTruncatedHistogram get();

        void addMemoryUsage(long memory);

        class Factory
                implements AccumulatorStateFactory<State>
        {
            @Override
            public State createSingleState()
            {
                return new SingleState();
            }

            @Override
            public Class<? extends State> getSingleStateClass()
            {
                return SingleState.class;
            }

            @Override
            public State createGroupedState()
            {
                return new GroupedState();
            }

            @Override
            public Class<? extends State> getGroupedStateClass()
            {
                return GroupedState.class;
            }

            public static class GroupedState
                    extends AbstractGroupedAccumulatorState
                    implements State
            {
                private final ObjectBigArray<TypedTruncatedHistogram> typedHistogram = new ObjectBigArray<>();
                private long size;

                @Override
                public void ensureCapacity(long size)
                {
                    typedHistogram.ensureCapacity(size);
                }

                @Override
                public TypedTruncatedHistogram get()
                {
                    return typedHistogram.get(getGroupId());
                }

                @Override
                public void set(TypedTruncatedHistogram value)
                {
                    requireNonNull(value, "value is null");

                    TypedTruncatedHistogram previous = get();
                    if (previous != null) {
                        size -= previous.getEstimatedSize();
                    }

                    typedHistogram.set(getGroupId(), value);
                    size += value.getEstimatedSize();
                }

                @Override
                public void addMemoryUsage(long memory)
                {
                    size += memory;
                }

                @Override
                public long getEstimatedSize()
                {
                    return size + typedHistogram.sizeOf();
                }
            }

            public static class SingleState
                    implements State
            {
                private TypedTruncatedHistogram typedHistogram;

                @Override
                public TypedTruncatedHistogram get()
                {
                    return typedHistogram;
                }

                @Override
                public void set(TypedTruncatedHistogram value)
                {
                    typedHistogram = value;
                }

                @Override
                public void addMemoryUsage(long memory)
                {
                }

                @Override
                public long getEstimatedSize()
                {
                    if (typedHistogram == null) {
                        return 0;
                    }
                    return typedHistogram.getEstimatedSize();
                }
            }
        }

        class Serializer
                implements AccumulatorStateSerializer<State> {
            private final Type type;

            public Serializer(Type type) {
                this.type = type;
            }

            @Override
            public Type getSerializedType() {
                return new RowType(ImmutableList.of(BIGINT, new MapType(type, BIGINT)), Optional.empty());
            }

            @Override
            public void serialize(State state, BlockBuilder out) {
                if (state.get() == null) {
                    out.appendNull();
                } else {
                    state.get().serialize(out);
                }
            }

            @Override
            public void deserialize(Block block, int index, State state) {
                if (block.isNull(index)) {
                    state.set(null);
                } else {
                    Block subBlock = (Block) getSerializedType().getObject(block, index);
                    state.set(TypedTruncatedHistogram.deserialize(type, subBlock));
                }
            }
        }
    }

    public TruncatedHistogram()
    {
        super(NAME, ImmutableList.of(comparableTypeParameter("K")), "map<K,bigint>", ImmutableList.of(StandardTypes.BIGINT, "K"));
    }

    @Override
    public String getDescription()
    {
        return "Count the number of times each value occurs, mixing up counts for infrequent values after max buckets";
    }

    @Override
    public InternalAggregationFunction specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = BigintType.BIGINT;
        return generateAggregation(keyType, valueType);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(TruncatedHistogram.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType);
        Type outputType = new MapType(keyType, valueType);
        AccumulatorStateSerializer<State> stateSerializer = new State.Serializer(keyType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType, inputTypes),
                createInputParameterMetadata(keyType),
                inputFunction,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                State.class,
                stateSerializer,
                new State.Factory(),
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, BIGINT),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, State state, long buckets, Block key, int position)
    {
        TypedTruncatedHistogram typedHistogram = state.get();
        if (typedHistogram == null) {
            checkCondition(buckets >= 2, INVALID_FUNCTION_ARGUMENT, "truncated_histogram bucket count must be greater than one");
            typedHistogram = new TypedTruncatedHistogram(type, EXPECTED_SIZE_FOR_HASHING, Ints.checkedCast(buckets));
            state.set(typedHistogram);
        }

        try {
            typedHistogram.add(position, key, 1L);
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
        }
    }

    public static void combine(State state, State otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            TypedTruncatedHistogram typedHistogram = state.get();
            try {
                typedHistogram.addAll(otherState.get());
            }
            catch (ExceededMemoryLimitException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
            }
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void output(State state, BlockBuilder out)
    {
        TypedTruncatedHistogram typedHistogram = state.get();
        if (typedHistogram == null) {
            out.appendNull();
        }
        else {
            typedHistogram.writeMapTo(out);
        }
    }

}
