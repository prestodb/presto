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

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.array.IntBigArray;
import io.prestosql.array.LongBigArray;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.aggregation.histogram.HashUtil.calculateMaxFill;
import static io.prestosql.operator.aggregation.histogram.HashUtil.computeBucketCount;
import static io.prestosql.operator.aggregation.histogram.HashUtil.nextBucketId;
import static io.prestosql.operator.aggregation.histogram.HashUtil.nextProbeLinear;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;

/**
 * helper class for {@link GroupedTypedHistogram}
 * May be used for other cases that need a simple hash for values
 * <p>
 * sort of a FlyWeightStore for values--will return unique number for a value. If it exists, you'll get the same number. Class map Value -> number
 * <p>
 * Note it assumes you're storing # -> Value (Type, Block, position, or the result of the ) somewhere else
 */
public class ValueStore
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedTypedHistogram.class).instanceSize();
    private static final float MAX_FILL_RATIO = 0.5f;
    private static final int EMPTY_BUCKET = -1;
    private final BlockBuilder values;
    private int rehashCount;

    private int mask;
    private int bucketCount;
    private IntBigArray buckets;
    private final LongBigArray valueHashes;
    private int maxFill;

    @VisibleForTesting
    public ValueStore(int expectedSize, BlockBuilder values)
    {
        bucketCount = computeBucketCount(expectedSize, MAX_FILL_RATIO);
        mask = bucketCount - 1;
        maxFill = calculateMaxFill(bucketCount, MAX_FILL_RATIO);
        this.values = values;
        buckets = new IntBigArray(-1);
        buckets.ensureCapacity(bucketCount);
        valueHashes = new LongBigArray(-1);
        valueHashes.ensureCapacity(bucketCount);
    }

    /**
     * This will add an item if not already in the system. It returns a pointer that is unique for multiple instances of the value. If item present,
     * returns the pointer into the system
     */
    public int addAndGetPosition(Type type, Block block, int position, long valueHash)
    {
        if (values.getPositionCount() >= maxFill) {
            rehash();
        }

        int bucketId = getBucketId(valueHash, mask);
        int valuePointer;

        // look for an empty slot or a slot containing this key
        int probeCount = 1;
        int originalBucketId = bucketId;
        while (true) {
            checkState(probeCount < bucketCount, "could not find match for value nor empty slot in %s buckets", bucketCount);
            valuePointer = buckets.get(bucketId);

            if (valuePointer == EMPTY_BUCKET) {
                valuePointer = values.getPositionCount();
                valueHashes.set(valuePointer, (int) valueHash);
                type.appendTo(block, position, values);
                buckets.set(bucketId, valuePointer);

                return valuePointer;
            }
            else if (type.equalTo(block, position, values, valuePointer)) {
                // value at position
                return valuePointer;
            }
            else {
                int probe = nextProbe(probeCount);
                bucketId = nextBucketId(originalBucketId, mask, probe);
                probeCount++;
            }
        }
    }

    private int getBucketId(long valueHash, int mask)
    {
        return (int) (valueHash & mask);
    }

    @VisibleForTesting
    void rehash()
    {
        ++rehashCount;

        long newBucketCountLong = bucketCount * 2L;

        if (newBucketCountLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed " + Integer.MAX_VALUE + " entries (" + newBucketCountLong + ")");
        }

        int newBucketCount = (int) newBucketCountLong;
        int newMask = newBucketCount - 1;

        IntBigArray newBuckets = new IntBigArray(-1);

        newBuckets.ensureCapacity(newBucketCount);

        for (int i = 0; i < values.getPositionCount(); i++) {
            long valueHash = valueHashes.get(i);
            int bucketId = getBucketId(valueHash, newMask);
            int probeCount = 1;

            while (newBuckets.get(bucketId) != EMPTY_BUCKET) {
                int probe = nextProbe(probeCount);

                bucketId = nextBucketId(bucketId, newMask, probe);
                probeCount++;
            }

            // record the mapping
            newBuckets.set(bucketId, i);
        }

        buckets = newBuckets;
        // worst case is every bucket has a unique value, so pre-emptively keep this large enough to have a value for ever bucket
        // TODO: could optimize the growth algorithm to be resize this only when necessary; this wastes memory but guarantees that if every value has a distinct hash, we have space
        valueHashes.ensureCapacity(newBucketCount);
        bucketCount = newBucketCount;
        maxFill = calculateMaxFill(newBucketCount, MAX_FILL_RATIO);
        mask = newMask;
    }

    public int getRehashCount()
    {
        return rehashCount;
    }

    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + buckets.sizeOf();
    }

    private int nextProbe(int probe)
    {
        return nextProbeLinear(probe);
    }
}
