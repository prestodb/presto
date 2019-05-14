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
package com.facebook.presto.operator;

import io.airlift.slice.SliceOutput;

import java.util.Arrays;

import static com.facebook.presto.operator.ByteArrayUtils.writeLengthPrefixedString;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class Int128ArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = "INT128_ARRAY";

    private byte[] valuesBuffer;
    private int valuesBufferIndex;

    Int128ArrayBlockEncodingBuffers(int initialElementCount)
    {
        this.initialBufferSize = initialBufferSize;
        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        if (valuesBuffer == null) {
            valuesBuffer = new byte[initialBufferSize * ARRAY_LONG_INDEX_SCALE * 2];
        }
        valuesBufferIndex = 0;
    }

    @Override
    protected void resetBuffers()
    {
        bufferedPositionCount = 0;
        valuesBufferIndex = 0;
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        nullsBufferContainsNull = false;
    }

    @Override
    protected void accumulateRowSizes(int[] rowSizes)
    {
        int averageElementSize = Long.BYTES * 2 + Byte.BYTES;

        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += averageElementSize;
        }
    }

    protected void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes)
    {
        int averageElementSize = Long.BYTES * 2 + Byte.BYTES;

        // it starts from 0 because top level offsets were normalized
        int lastOffset = 0;
        for (int i = 0; i < positionCount; i++) {
            int currentOffset = positionOffsets[i];
            int entryCount = currentOffset - lastOffset;
            rowSizes[i] += entryCount * averageElementSize;

            lastOffset = currentOffset;
        }
    }

    @Override
    protected void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        verify(decodedBlock != null);

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;
        verify(positionsOffset + batchSize - 1 < positions.length && positions[positionsOffset] >= 0 &&
                positions[positionsOffset + batchSize - 1] < decodedBlock.getPositionCount());

        appendValuesToBuffer();
        appendNulls();
        bufferedPositionCount += batchSize;
    }

    @Override
    protected void writeTo(SliceOutput sliceOutput)
    {
        // TODO: getSizeInBytes() was calculated in flush and doesnt need to recalculated
        verify(getSizeInBytes() <= sliceOutput.writableBytes());

        writeLengthPrefixedString(sliceOutput, NAME);
        sliceOutput.writeInt(bufferedPositionCount);
        writeNullsTo(sliceOutput);
        sliceOutput.appendBytes(valuesBuffer, 0, valuesBufferIndex);
    }

    @Override
    protected int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +    // NAME
                SIZE_OF_INT +                   // positionCount
                SIZE_OF_BYTE +                  // nulls uses 1 byte for mayHaveNull
                nullsBufferIndex +              // nulls buffer
                (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0) +  // the remaining nulls not serialized yet
                valuesBufferIndex;              // values buffer
    }

    private void appendValuesToBuffer()
    {
        verify(decodedBlock != null);

        ensureValueBufferSize();

        if (decodedBlock.mayHaveNull()) {
            appendIpAddresesWithNullsToBuffer();
        }
        else {
            appendIpAddresesToBuffer();
        }
    }

    private void appendIpAddresesToBuffer()
    {
        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];

            long value = decodedBlock.getLong(position, 0);
            ByteArrayUtils.writeLong(valuesBuffer, valuesBufferIndex, value);
            valuesBufferIndex += ARRAY_LONG_INDEX_SCALE;

            value = decodedBlock.getLong(position, 8);
            ByteArrayUtils.writeLong(valuesBuffer, valuesBufferIndex, value);
            valuesBufferIndex += ARRAY_LONG_INDEX_SCALE;
        }
    }

    private void appendIpAddresesWithNullsToBuffer()
    {
        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];

            long value = decodedBlock.getLong(position, 0);
            ByteArrayUtils.writeLong(valuesBuffer, valuesBufferIndex, value);
            valuesBufferIndex += ARRAY_LONG_INDEX_SCALE;

            value = decodedBlock.getLong(position, 8);
            ByteArrayUtils.writeLong(valuesBuffer, valuesBufferIndex, value);
            valuesBufferIndex += ARRAY_LONG_INDEX_SCALE;

            if (decodedBlock.isNull(position)) {
                valuesBufferIndex -= ARRAY_LONG_INDEX_SCALE * 2;
            }
        }
    }

    private void ensureValueBufferSize()
    {
        verify(valuesBuffer != null);

        int requiredSize = valuesBufferIndex + batchSize * ARRAY_LONG_INDEX_SCALE * 2;
        if (requiredSize > valuesBuffer.length) {
            valuesBuffer = Arrays.copyOf(valuesBuffer, max(valuesBuffer.length * 2, requiredSize));
        }
    }
}
