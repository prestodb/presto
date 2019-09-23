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

public class DefinitionLevelDecoder
        extends BaseRLEBitPackedDecoder
{
    public DefinitionLevelDecoder(int valueCount, int bitWidth, InputStream in)
    {
        super(valueCount, bitWidth, in);
    }

    public DefinitionLevelDecoder(int rleValue, int valueCount)
    {
        super(rleValue, valueCount);
    }

    public void readNext(int[] values, int offset, int length)
            throws IOException
    {
        int destIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (this.currentCount == 0) {
                if (!this.readNext()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remainingToCopy, this.currentCount);
            switch (this.mode) {
                case RLE: {
                    int rleValue = this.currentValue;
                    int endIndex = destIndex + readChunkSize;
                    while (destIndex < endIndex) {
                        values[destIndex] = rleValue;
                        destIndex++;
                    }
                    break;
                }
                case PACKED: {
                    System.arraycopy(currentBuffer, currentBuffer.length - this.currentCount, values, destIndex, readChunkSize);
                    destIndex += readChunkSize;
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }
            this.currentCount -= readChunkSize;
            remainingToCopy -= readChunkSize;
        }

        checkState(remainingToCopy == 0, "Failed to copy the requested number of DLs");
    }
}
