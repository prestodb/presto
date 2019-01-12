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
package io.prestosql.operator.aggregation.histogram;

import io.prestosql.array.IntBigArray;
import io.prestosql.array.LongBigArray;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.type.TypeUtils;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

public class SingleTypedHistogram
        implements TypedHistogram
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleTypedHistogram.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;

    private final int expectedSize;
    private int hashCapacity;
    private int maxFill;
    private int mask;

    private final Type type;
    private final BlockBuilder values;

    private IntBigArray hashPositions;
    private final LongBigArray counts;

    private SingleTypedHistogram(Type type, int expectedSize, int hashCapacity, BlockBuilder values)
    {
        this.type = type;
        this.expectedSize = expectedSize;
        this.hashCapacity = hashCapacity;
        this.values = values;

        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashPositions = new IntBigArray(-1);
        hashPositions.ensureCapacity(hashCapacity);
        counts = new LongBigArray();
        counts.ensureCapacity(hashCapacity);
    }

    public SingleTypedHistogram(Type type, int expectedSize)
    {
        this(type, expectedSize, computeBucketCount(expectedSize), type.createBlockBuilder(null, computeBucketCount(expectedSize)));
    }

    private static int computeBucketCount(int expectedSize)
    {
        return arraySize(expectedSize, FILL_RATIO);
    }

    public SingleTypedHistogram(Block block, Type type, int expectedSize)
    {
        this(type, expectedSize);
        requireNonNull(block, "block is null");
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(i, block, BIGINT.getLong(block, i + 1));
        }
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + values.getRetainedSizeInBytes() + counts.sizeOf() + hashPositions.sizeOf();
    }

    @Override
    public void serialize(BlockBuilder out)
    {
        if (values.getPositionCount() == 0) {
            out.appendNull();
        }
        else {
            Block valuesBlock = values.build();
            BlockBuilder blockBuilder = out.beginBlockEntry();
            for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
                type.appendTo(valuesBlock, i, blockBuilder);
                BIGINT.writeLong(blockBuilder, counts.get(i));
            }
            out.closeEntry();
        }
    }

    @Override
    public void addAll(TypedHistogram other)
    {
        other.readAllValues((block, position, count) -> add(position, block, count));
    }

    @Override
    public void readAllValues(HistogramValueReader reader)
    {
        for (int i = 0; i < values.getPositionCount(); i++) {
            long count = counts.get(i);
            if (count > 0) {
                reader.read(values, i, count);
            }
        }
    }

    @Override
    public void add(int position, Block block, long count)
    {
        int hashPosition = getBucketId(TypeUtils.hashPosition(type, block, position), mask);

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

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int getExpectedSize()
    {
        return expectedSize;
    }

    @Override
    public boolean isEmpty()
    {
        return values.getPositionCount() == 0;
    }

    private void addNewGroup(int hashPosition, int position, Block block, long count)
    {
        hashPositions.set(hashPosition, values.getPositionCount());
        counts.set(values.getPositionCount(), count);
        type.appendTo(block, position, values);

        // increase capacity, if necessary
        if (values.getPositionCount() >= maxFill) {
            rehash();
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;

        int newMask = newCapacity - 1;
        IntBigArray newHashPositions = new IntBigArray(-1);
        newHashPositions.ensureCapacity(newCapacity);

        for (int i = 0; i < values.getPositionCount(); i++) {
            // find an empty slot for the address
            int hashPosition = getBucketId(TypeUtils.hashPosition(type, values, i), newMask);

            while (newHashPositions.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & newMask;
            }

            // record the mapping
            newHashPositions.set(hashPosition, i);
        }

        hashCapacity = newCapacity;
        mask = newMask;
        maxFill = calculateMaxFill(newCapacity);
        hashPositions = newHashPositions;

        this.counts.ensureCapacity(maxFill);
    }

    private static int getBucketId(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }
}
