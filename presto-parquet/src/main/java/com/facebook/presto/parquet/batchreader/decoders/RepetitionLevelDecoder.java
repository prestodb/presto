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

import com.facebook.presto.parquet.batchreader.decoders.rle.GenericRLEDictionaryValuesDecoder;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.io.ParquetDecodingException;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.InputStream;

import static io.airlift.slice.SizeOf.sizeOf;

public class RepetitionLevelDecoder
        extends GenericRLEDictionaryValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GenericRLEDictionaryValuesDecoder.class).instanceSize();

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

            switch (getCurrentMode()) {
                case RLE: {
                    int rleValue = getDecodedInt();
                    if (rleValue == 0) {
                        int chunkSize = Math.min(remainingToCopy, getCurrentCount());
                        for (int i = 0; i < chunkSize; i++) {
                            repetitionLevels.add(0);
                        }
                        decrementCurrentCount(chunkSize);
                        remaining -= chunkSize;
                        remainingToCopy -= chunkSize;
                    }
                    else {
                        remaining -= getCurrentCount();
                        for (int i = 0; i < getCurrentCount(); i++) {
                            repetitionLevels.add(rleValue);
                        }
                        decrementCurrentCount(getCurrentCount());
                    }
                    break;
                }
                case PACKED: {
                    final int[] localBuffer = getDecodedInts();
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
                    decrementCurrentCount(endOffsetPackedBuffer - currentOffsetPackedBuffer);
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + getCurrentMode());
            }
        }
        return batchSize - remainingToCopy;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(getDecodedInts());
    }

    private boolean ensureBlockAvailable()
            throws IOException
    {
        if (getCurrentCount() == 0) {
            if (!decode()) {
                return false;
            }
            decrementCurrentCount(Math.min(remaining, getCurrentCount()));
            currentOffsetPackedBuffer = 0;
            endOffsetPackedBuffer = getCurrentCount();
        }
        return true;
    }
}
