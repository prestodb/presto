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
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;
import com.facebook.presto.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
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

    protected AbstractLongSelectiveStreamReader(Optional<Type> outputType)
    {
        this.outputRequired = outputType.isPresent();
        this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    protected Block buildOutputBlock(int[] positions, int positionCount, boolean includeNulls)
    {
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
        if (values == null || values.length < capacity) {
            values = new long[capacity];
        }

        if (recordNulls) {
            if (nulls == null || nulls.length < capacity) {
                nulls = new boolean[capacity];
            }
        }
    }

    protected void ensureOutputPositionsCapacity(int capacity)
    {
        if (outputPositions == null || outputPositions.length < capacity) {
            outputPositions = new int[capacity];
        }
    }
}
