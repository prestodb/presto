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

    public RepetitionLevelDecoder(int valueCount, int bitWidth, InputStream in)
    {
        super(valueCount, bitWidth, in);
        this.remaining = valueCount;
    }

    public RepetitionLevelDecoder(int rleValue, int valueCount)
    {
        super(rleValue, valueCount);
    }

    public int readNext(IntList rls, int batchSize)
            throws IOException
    {
        int remainingToCopy = batchSize;
        while (remainingToCopy > 0) {
            if (!ensureBlockAvailable()) {
                break;
            }

            switch (this.mode) {
                case RLE: {
                    int rleValue = this.currentValue;
                    if (rleValue == 0) {
                        int readChunkSize = Math.min(remainingToCopy, this.currentCount);
                        for (int i = 0; i < readChunkSize; i++) {
                            rls.add(0);
                        }
                        this.currentCount -= readChunkSize;
                        this.remaining -= readChunkSize;
                        remainingToCopy -= readChunkSize;
                    }
                    else {
                        this.remaining -= currentCount;
                        for (int i = 0; i < currentCount; i++) {
                            rls.add(rleValue);
                        }
                        currentCount = 0;
                    }
                    break;
                }
                case PACKED: {
                    final int[] localCurrentBuffer = this.currentBuffer;
                    do {
                        int rlValue = localCurrentBuffer[currentOffsetPackedBuffer++];
                        rls.add(rlValue);
                        if (rlValue == 0) {
                            remainingToCopy--;
                        }
                        this.remaining--;
                    }
                    while (currentOffsetPackedBuffer < endOffsetPackedBuffer && remainingToCopy > 0);
                    this.currentCount = endOffsetPackedBuffer - currentOffsetPackedBuffer;
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }
        }

        return batchSize - remainingToCopy;
    }

    private boolean ensureBlockAvailable()
            throws IOException
    {
        if (this.currentCount == 0) {
            if (!readNext()) {
                return false;
            }
            this.currentCount = Math.min(this.remaining, this.currentCount);
            this.currentOffsetPackedBuffer = 0;
            this.endOffsetPackedBuffer = this.currentCount;
        }

        return true;
    }
}
