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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.Int128ArrayBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import io.airlift.slice.Slice;
import io.airlift.slice.UnsafeSlice;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.rescale;

public class LongDecimalSelectiveStreamReader
        extends AbstractDecimalSelectiveStreamReader
{
    public LongDecimalSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            OrcLocalMemoryContext systemMemoryContext)
    {
        super(streamDescriptor, filter, outputType, systemMemoryContext, 2);
    }

    @Override
    protected int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();

        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                nulls[i] = true;
            }
            else {
                int scale = (int) scaleStream.next();
                dataStream.nextLongDecimal(decimal);
                rescale(decimal, this.scale - scale, rescaledDecimal);
                values[2 * i] = UnsafeSlice.getLongUnchecked(rescaledDecimal, 0);
                values[2 * i + 1] = UnsafeSlice.getLongUnchecked(rescaledDecimal, Long.BYTES);
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    @Override
    protected int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        int streamPosition = 0;
        outputPositionCount = 0;
        Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if ((nonDeterministicFilter && filter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                int scale = (int) scaleStream.next();
                dataStream.nextLongDecimal(decimal);
                rescale(decimal, this.scale - scale, rescaledDecimal);
                long low = UnsafeSlice.getLongUnchecked(rescaledDecimal, 0);
                long high = UnsafeSlice.getLongUnchecked(rescaledDecimal, Long.BYTES);
                if (filter.testDecimal(low, high)) {
                    if (outputRequired) {
                        values[2 * outputPositionCount] = low;
                        values[2 * outputPositionCount + 1] = high;
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            streamPosition++;

            if (filter != null) {
                outputPositionCount -= filter.getPrecedingPositionsToFail();
                int succeedingPositionsToFail = filter.getSucceedingPositionsToFail();
                if (succeedingPositionsToFail > 0) {
                    int positionsToSkip = 0;
                    for (int j = 0; j < succeedingPositionsToFail; j++) {
                        i++;
                        int nextPosition = positions[i];
                        positionsToSkip += 1 + nextPosition - streamPosition;
                        streamPosition = nextPosition + 1;
                    }
                    skip(positionsToSkip);
                }
            }
        }
        return streamPosition;
    }

    @Override
    protected void copyValues(int[] positions, int positionsCount, long[] valuesCopy, boolean[] nullsCopy)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            valuesCopy[2 * positionIndex] = this.values[2 * i];
            valuesCopy[2 * positionIndex + 1] = this.values[2 * i + 1];

            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }
            positionIndex++;

            if (positionIndex >= positionsCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }
    }

    @Override
    protected void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            values[2 * positionIndex] = values[2 * i];
            values[2 * positionIndex + 1] = values[2 * i + 1];
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

    @Override
    protected Block makeBlock(int positionCount, boolean includeNulls, boolean[] nulls, long[] values)
    {
        return new Int128ArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
    }
}
