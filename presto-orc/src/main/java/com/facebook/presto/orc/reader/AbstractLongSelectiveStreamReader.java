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
package com.facebook.presto.orc.reader;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.ClosingBlockLease;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;
import com.facebook.presto.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

abstract class AbstractLongSelectiveStreamReader
        implements SelectiveStreamReader
{
    protected final boolean outputRequired;
    @Nullable
    protected final Type outputType;

    @Nullable
    protected long[] values;
    @Nullable
    protected boolean[] nulls;
    @Nullable
    protected int[] outputPositions;
    protected int outputPositionCount;

    private int[] intValues;
    private boolean intValuesPopulated;

    private short[] shortValues;
    private boolean shortValuesPopulated;

    private boolean valuesInUse;

    protected AbstractLongSelectiveStreamReader(Optional<Type> outputType)
    {
        this.outputRequired = outputType.isPresent();
        this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
    }

    protected void prepareNextRead(int positionCount, boolean withNulls)
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (outputRequired) {
            ensureValuesCapacity(positionCount, withNulls);
        }
        intValuesPopulated = false;
        shortValuesPopulated = false;
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
    }

    protected BlockLease buildOutputBlockView(int[] positions, int positionCount, boolean includeNulls)
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (outputType == BIGINT) {
            if (positionCount < outputPositionCount) {
                compactValues(positions, positionCount, includeNulls);
            }
            return newLease(new LongArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values));
        }

        if (outputType == INTEGER || outputType == DATE) {
            if (!intValuesPopulated || positionCount < outputPositionCount) {
                if (positionCount < outputPositionCount) {
                    compactValues(positions, positionCount, includeNulls);
                }
                if (intValues == null || intValues.length < positionCount) {
                    intValues = new int[positionCount];
                }
                for (int i = 0; i < positionCount; i++) {
                    intValues[i] = (int) values[i];
                }
                intValuesPopulated = true;
            }

            return newLease(new IntArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), intValues));
        }

        if (outputType == SMALLINT) {
            if (!shortValuesPopulated || positionCount < outputPositionCount) {
                if (positionCount < outputPositionCount) {
                    compactValues(positions, positionCount, includeNulls);
                }
                if (shortValues == null || shortValues.length < positionCount) {
                    shortValues = new short[positionCount];
                }
                for (int i = 0; i < positionCount; i++) {
                    shortValues[i] = (short) values[i];
                }
                shortValuesPopulated = true;
            }

            return newLease(new ShortArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), shortValues));
        }

        throw new UnsupportedOperationException("Unsupported type: " + outputType);
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    protected Block buildOutputBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (outputType == BIGINT) {
            return getLongArrayBlock(positions, positionCount, includeNulls);
        }

        if (outputType == INTEGER || outputType == DATE) {
            return getIntArrayBlock(positions, positionCount, includeNulls);
        }

        if (outputType == SMALLINT) {
            return getShortArrayBlock(positions, positionCount, includeNulls);
        }

        throw new UnsupportedOperationException("Unsupported type: " + outputType);
    }

    private Block getLongArrayBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        if (positionCount == outputPositionCount) {
            LongArrayBlock block;
            if (includeNulls) {
                block = new LongArrayBlock(positionCount, Optional.ofNullable(nulls), values);
                nulls = null;
            }
            else {
                block = new LongArrayBlock(positionCount, Optional.empty(), values);
            }
            values = null;
            return block;
        }

        long[] valuesCopy = new long[positionCount];
        boolean[] nullsCopy = null;

        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            valuesCopy[positionIndex] = this.values[i];
            if (includeNulls) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new LongArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    private Block getIntArrayBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        if (intValuesPopulated && positionCount == outputPositionCount) {
            return new IntArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), intValues);
        }

        int[] valuesCopy = new int[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            valuesCopy[positionIndex] = toIntExact(this.values[i]);
            if (includeNulls) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new IntArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    private Block getShortArrayBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        if (shortValuesPopulated && positionCount == outputPositionCount) {
            return new ShortArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), shortValues);
        }

        short[] valuesCopy = new short[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            valuesCopy[positionIndex] = (short) this.values[i];
            if (includeNulls) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new ShortArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    protected void ensureValuesCapacity(int capacity, boolean recordNulls)
    {
        values = ensureCapacity(values, capacity);

        if (recordNulls) {
            nulls = ensureCapacity(nulls, capacity);
        }
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            values[positionIndex] = values[i];
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }
}
