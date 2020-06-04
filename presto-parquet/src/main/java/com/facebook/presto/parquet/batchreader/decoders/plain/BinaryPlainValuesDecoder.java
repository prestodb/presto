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
package com.facebook.presto.parquet.batchreader.decoders.plain;

import com.facebook.presto.parquet.batchreader.BytesUtils;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class BinaryPlainValuesDecoder
        implements BinaryValuesDecoder
{
    private final byte[] buffer;
    private final int bufEnd;

    private int bufOffset;

    public BinaryPlainValuesDecoder(byte[] buffer, int bufOffset, int bufLength)
    {
        this.buffer = buffer;
        this.bufEnd = bufOffset + bufLength;
        this.bufOffset = bufOffset;
    }

    @Override
    public ValueBuffer readNext(int length)
    {
        int remaining = length;
        int[] offsets = new int[length + 1];
        int offsetIndex = 0;
        int bufferSize = 0;
        while (remaining > 0 && bufOffset < bufEnd) {
            int size = BytesUtils.getInt(buffer, bufOffset);
            offsets[offsetIndex++] = bufOffset;
            bufOffset += (4 + size);
            bufferSize += size;
            remaining--;
        }
        offsets[offsetIndex] = bufOffset;

        return new PlainValueBuffer(bufferSize, offsets);
    }

    @Override
    public int readIntoBuffer(byte[] byteBuffer, int bufferIndex, int[] offsets, int offsetIndex, ValueBuffer valueBuffer)
    {
        checkArgument(byteBuffer.length - bufferIndex >= valueBuffer.getBufferSize(), "not enough space in the input buffer");

        PlainValueBuffer plainValueBuffer = (PlainValueBuffer) valueBuffer;
        final int[] sourceOffsets = plainValueBuffer.getSourceOffsets();
        final int numEntries = sourceOffsets.length - 1;

        for (int i = 0; i < numEntries; i++) {
            offsets[offsetIndex++] = bufferIndex;
            int length = sourceOffsets[i + 1] - (sourceOffsets[i] + 4);
            System.arraycopy(buffer, sourceOffsets[i] + 4, byteBuffer, bufferIndex, length);
            bufferIndex += length;
        }
        offsets[offsetIndex] = bufferIndex;
        return bufferIndex;
    }

    @Override
    public void skip(int length)
    {
        int remaining = length;
        while (remaining > 0 && bufOffset < bufEnd) {
            int size = BytesUtils.getInt(buffer, bufOffset);
            bufOffset += (4 + size);
            remaining--;
        }
        checkState(remaining == 0, "Invalid read size request");
    }

    public static class PlainValueBuffer
            implements ValueBuffer
    {
        private final int bufferSize;
        private final int[] sourceOffsets;

        public PlainValueBuffer(int bufferSize, int[] sourceOffsets)
        {
            this.bufferSize = bufferSize;
            this.sourceOffsets = sourceOffsets;
        }

        @Override
        public int getBufferSize()
        {
            return bufferSize;
        }

        public int[] getSourceOffsets()
        {
            return sourceOffsets;
        }
    }
}
