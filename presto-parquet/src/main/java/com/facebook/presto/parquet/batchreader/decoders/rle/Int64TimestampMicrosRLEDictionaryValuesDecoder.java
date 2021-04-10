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
package com.facebook.presto.parquet.batchreader.decoders.rle;

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64TimestampMicrosValuesDecoder;
import com.facebook.presto.parquet.dictionary.LongDictionary;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class Int64TimestampMicrosRLEDictionaryValuesDecoder
        extends BaseRLEBitPackedDecoder
        implements Int64TimestampMicrosValuesDecoder
{
    private final LongDictionary dictionary;

    public Int64TimestampMicrosRLEDictionaryValuesDecoder(int bitWidth, InputStream inputStream, LongDictionary dictionary)
    {
        super(Integer.MAX_VALUE, bitWidth, inputStream);
        this.dictionary = dictionary;
    }

    @Override
    public void readNext(long[] values, int offset, int length)
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

            int numEntriesToFill = Math.min(remainingToCopy, currentCount);
            int endIndex = destinationIndex + numEntriesToFill;
            switch (mode) {
                case RLE: {
                    final int rleValue = currentValue;
                    final long rleDictionaryValue = MICROSECONDS.toMillis(dictionary.decodeToLong(rleValue));
                    while (destinationIndex < endIndex) {
                        values[destinationIndex++] = rleDictionaryValue;
                    }
                    break;
                }
                case PACKED: {
                    final int[] localBuffer = currentBuffer;
                    final LongDictionary localDictionary = dictionary;
                    for (int srcIndex = currentBuffer.length - currentCount; destinationIndex < endIndex; srcIndex++) {
                        long dictionaryValue = localDictionary.decodeToLong(localBuffer[srcIndex]);
                        values[destinationIndex++] = MICROSECONDS.toMillis(dictionaryValue);
                    }
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + mode);
            }

            currentCount -= numEntriesToFill;
            remainingToCopy -= numEntriesToFill;
        }

        checkState(remainingToCopy == 0, "End of stream: Invalid read size request");
    }

    @Override
    public void skip(int length)
            throws IOException
    {
        checkArgument(length >= 0, "invalid length %s", length);
        int remaining = length;
        while (remaining > 0) {
            if (currentCount == 0) {
                if (!decode()) {
                    break;
                }
            }

            int chunkSize = Math.min(remaining, currentCount);
            currentCount -= chunkSize;
            remaining -= chunkSize;
        }

        checkState(remaining == 0, "End of stream: Invalid skip size request: %s", length);
    }
}
