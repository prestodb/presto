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
package com.facebook.presto.parquet.batchreader.decoders;

import com.facebook.presto.parquet.batchreader.decoders.rle.BaseRLEBitPackedDecoder;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkState;

/**
 * Definition Level decoder for non-nested types where the values are either 0 or 1
 */
public class FlatDefinitionLevelDecoder
        extends BaseRLEBitPackedDecoder
{
    public FlatDefinitionLevelDecoder(int valueCount, InputStream in)
    {
        super(valueCount, 1, in);
    }

    public FlatDefinitionLevelDecoder(int rleValue, int valueCount)
    {
        super(rleValue, valueCount);
    }

    public int readNext(boolean[] values, int offset, int length)
            throws IOException
    {
        int nonNullCount = 0;
        int destinationIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (this.currentCount == 0) {
                if (!this.readNext()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingToCopy, this.currentCount);
            int endIndex = destinationIndex + readChunkSize;
            switch (this.mode) {
                case RLE: {
                    boolean rleValue = this.currentValue == 0;
                    while (destinationIndex < endIndex) {
                        values[destinationIndex++] = rleValue;
                    }
                    nonNullCount += this.currentValue * readChunkSize;
                    break;
                }
                case PACKED: {
                    int[] currentBuffer = this.currentBuffer;
                    for (int sourceIndex = currentBuffer.length - this.currentCount; destinationIndex < endIndex; sourceIndex++, destinationIndex++) {
                        final int value = currentBuffer[sourceIndex];
                        values[destinationIndex] = value == 0;
                        nonNullCount += value;
                    }
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }
            this.currentCount -= readChunkSize;
            remainingToCopy -= readChunkSize;
        }

        checkState(remainingToCopy == 0, "Failed to copy the requested number of definition levels");
        return nonNullCount;
    }
}
