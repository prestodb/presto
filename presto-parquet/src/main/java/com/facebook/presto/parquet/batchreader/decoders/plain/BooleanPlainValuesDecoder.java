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
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BooleanValuesDecoder;

import static com.google.common.base.Preconditions.checkState;

public class BooleanPlainValuesDecoder
        implements BooleanValuesDecoder
{
    private final byte[] byteBuffer;
    private final int bufferEnd;

    private int bufferOffset;
    private int currentByteOffset;
    private byte currentByte;

    public BooleanPlainValuesDecoder(byte[] byteBuffer, int bufferOffset, int bufferLength)
    {
        this.byteBuffer = byteBuffer;
        this.bufferEnd = bufferOffset + bufferLength;
        this.bufferOffset = bufferOffset;
    }

    public void readNext(byte[] values, int offset, int length)
    {
        // Stream bounds check
        if (currentByteOffset > 0) {
            // read from the partial values remaining in current byte
            int readChunk = Math.min(length, 8 - currentByteOffset);

            final byte inValue = currentByte;
            for (int i = 0; i < readChunk; i++) {
                values[offset++] = (byte) (inValue >> currentByteOffset & 1);
                currentByteOffset++;
            }

            length -= readChunk;
            currentByteOffset = currentByteOffset % 8;
        }

        checkState(bufferOffset + (length / 8) <= bufferEnd, "End of stream: invalid read request");

        while (length >= 8) {
            BytesUtils.unpack8Values(byteBuffer[bufferOffset++], values, offset);
            length -= 8;
            offset += 8;
        }

        if (length > 0) {
            checkState(bufferOffset < bufferEnd, "End of stream: invalid read request");
            // read partial values from current byte until the requested length is satisfied
            byte inValue = byteBuffer[bufferOffset++];
            for (byte i = 0; i < length; i++) {
                values[offset++] = (byte) (inValue >> i & 1);
            }

            currentByte = inValue;
            currentByteOffset = length;
        }
    }

    @Override
    public void skip(int length)
    {
        if (currentByteOffset > 0) {
            // skip from the partial values remaining in current byte
            int readChunkSize = Math.min(length, 8 - currentByteOffset);
            length -= readChunkSize;
            currentByteOffset = (currentByteOffset + readChunkSize) % 8;
        }

        int fullBytes = length / 8;

        if (fullBytes > 0) {
            checkState(bufferOffset + fullBytes <= bufferEnd, "End of stream: invalid read request");
            bufferOffset += fullBytes;
        }

        length = length % 8;

        if (length > 0) {
            // skip partial values from current byte until the requested length is satisfied
            checkState(bufferOffset < bufferEnd, "End of stream: invalid read request");
            currentByte = byteBuffer[bufferOffset++];
            currentByteOffset = length;
        }
    }
}
