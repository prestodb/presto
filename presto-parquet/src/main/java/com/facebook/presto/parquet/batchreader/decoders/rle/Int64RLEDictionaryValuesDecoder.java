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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64ValuesDecoder;
import com.facebook.presto.parquet.dictionary.LongDictionary;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Int64RLEDictionaryValuesDecoder
        extends BaseRLEBitPackedDecoder
        implements Int64ValuesDecoder
{
    private final LongDictionary dictionary;

    public Int64RLEDictionaryValuesDecoder(int bitWidth, InputStream in, LongDictionary dictionary)
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
                    final long rleDictionaryValue = dictionary.decodeToLong(rleValue);
                    while (destIndex < endIndex) {
                        values[destIndex++] = rleDictionaryValue;
                    }
                    break;
                }
                case PACKED: {
                    final int[] localCurrentBuffer = this.currentBuffer;
                    final LongDictionary localDictionary = this.dictionary;
                    for (int srcIndex = this.currentBuffer.length - this.currentCount; destIndex < endIndex; srcIndex++) {
                        long dictionaryValue = localDictionary.decodeToLong(localCurrentBuffer[srcIndex]);
                        values[destIndex++] = dictionaryValue;
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
