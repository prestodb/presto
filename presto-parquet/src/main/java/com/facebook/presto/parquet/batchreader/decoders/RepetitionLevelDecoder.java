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
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

public class RepetitionLevelDecoder
        extends BaseRLEBitPackedDecoder
{
    private int remaining;
    private int currentOffsetPackedBuffer;
    private int endOffsetPackedBuffer;

    public RepetitionLevelDecoder(int valueCount, int bitWidth, InputStream inputStream)
    {
        super(valueCount, bitWidth, inputStream);
        this.remaining = valueCount;
    }

    public RepetitionLevelDecoder(int rleValue, int valueCount)
    {
        super(rleValue, valueCount);
    }

    public int readNext(IntList repetitionLevels, int batchSize)
            throws IOException
    {
        int remainingToCopy = batchSize;
        while (remainingToCopy > 0) {
            if (!ensureBlockAvailable()) {
                break;
            }

            switch (mode) {
                case RLE: {
                    int rleValue = currentValue;
                    if (rleValue == 0) {
                        int chunkSize = Math.min(remainingToCopy, currentCount);
                        for (int i = 0; i < chunkSize; i++) {
                            repetitionLevels.add(0);
                        }
                        currentCount -= chunkSize;
                        remaining -= chunkSize;
                        remainingToCopy -= chunkSize;
                    }
                    else {
                        remaining -= currentCount;
                        for (int i = 0; i < currentCount; i++) {
                            repetitionLevels.add(rleValue);
                        }
                        currentCount = 0;
                    }
                    break;
                }
                case PACKED: {
                    final int[] localBuffer = currentBuffer;
                    do {
                        int rlValue = localBuffer[currentOffsetPackedBuffer];
                        currentOffsetPackedBuffer = currentOffsetPackedBuffer + 1;
                        repetitionLevels.add(rlValue);
                        if (rlValue == 0) {
                            remainingToCopy--;
                        }
                        remaining--;
                    }
                    while (currentOffsetPackedBuffer < endOffsetPackedBuffer && remainingToCopy > 0);
                    currentCount = endOffsetPackedBuffer - currentOffsetPackedBuffer;
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + mode);
            }
        }
        return batchSize - remainingToCopy;
    }

    private boolean ensureBlockAvailable()
            throws IOException
    {
        if (currentCount == 0) {
            if (!decode()) {
                return false;
            }
            currentCount = Math.min(remaining, currentCount);
            currentOffsetPackedBuffer = 0;
            endOffsetPackedBuffer = currentCount;
        }
        return true;
    }
}
