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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.TimestampValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.TimestampDictionary;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class TimestampRLEDictionaryValuesDecoder
        extends BaseRLEBitPackedDecoder
        implements TimestampValuesDecoder
{
    private final TimestampDictionary dictionary;

    public TimestampRLEDictionaryValuesDecoder(int bitWidth, InputStream in, TimestampDictionary dictionary)
    {
        super(Integer.MAX_VALUE, bitWidth, in);
        this.dictionary = dictionary;
    }

    @Override
    public void readNext(long[] values, int offset, int length)
            throws IOException
    {
        int destIndex = offset;
        int remainingToCopy = length;
        while (remainingToCopy > 0) {
            if (this.currentCount == 0) {
                if (!readNext()) {
                    break;
                }
            }

            int numEntriesToFill = Math.min(remainingToCopy, this.currentCount);
            int endIndex = destIndex + numEntriesToFill;
            switch (this.mode) {
                case RLE: {
                    final int rleValue = this.currentValue;
                    final long rleValueMillis = dictionary.decodeToLong(rleValue);
                    while (destIndex < endIndex) {
                        values[destIndex++] = rleValueMillis;
                    }
                    break;
                }
                case PACKED: {
                    final int[] localCurrentBuffer = this.currentBuffer;
                    final TimestampDictionary localDictionary = this.dictionary;
                    for (int srcIndex = this.currentBuffer.length - this.currentCount; destIndex < endIndex; srcIndex++) {
                        values[destIndex++] = localDictionary.decodeToLong(localCurrentBuffer[srcIndex]);
                    }
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }

            this.currentCount -= numEntriesToFill;
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
            if (this.currentCount == 0) {
                if (!readNext()) {
                    break;
                }
            }

            int readChunkSize = Math.min(remaining, this.currentCount);
            this.currentCount -= readChunkSize;
            remaining -= readChunkSize;
        }

        checkState(remaining == 0, "End of stream: Invalid skip size request: %s", length);
    }
}
