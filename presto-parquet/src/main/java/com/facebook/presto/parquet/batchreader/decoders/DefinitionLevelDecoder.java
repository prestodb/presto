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
    public DefinitionLevelDecoder(int valueCount, int bitWidth, InputStream inputStream)
    {
        super(valueCount, bitWidth, inputStream);
    }

    public DefinitionLevelDecoder(int rleValue, int valueCount)
    {
        super(rleValue, valueCount);
    }

    public void readNext(int[] values, int offset, int length)
            throws IOException
    {
        int destinationIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (currentCount == 0) {
                if (!decode()) {
                    break;
                }
            }

            int chunkSize = Math.min(remainingToCopy, currentCount);
            switch (mode) {
                case RLE: {
                    int rleValue = currentValue;
                    int endIndex = destinationIndex + chunkSize;
                    while (destinationIndex < endIndex) {
                        values[destinationIndex] = rleValue;
                        destinationIndex++;
                    }
                    break;
                }
                case PACKED: {
                    System.arraycopy(currentBuffer, currentBuffer.length - currentCount, values, destinationIndex, chunkSize);
                    destinationIndex += chunkSize;
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + mode);
            }
            currentCount -= chunkSize;
            remainingToCopy -= chunkSize;
        }
        checkState(remainingToCopy == 0, "Failed to copy the requested number of DLs");
    }
}
