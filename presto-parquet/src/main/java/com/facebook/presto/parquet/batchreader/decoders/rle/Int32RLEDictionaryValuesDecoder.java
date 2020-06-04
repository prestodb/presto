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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int32ValuesDecoder;
import com.facebook.presto.parquet.dictionary.IntegerDictionary;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Int32RLEDictionaryValuesDecoder
        extends BaseRLEBitPackedDecoder
        implements Int32ValuesDecoder
{
    private final IntegerDictionary dictionary;

    public Int32RLEDictionaryValuesDecoder(int bitWidth, InputStream in, IntegerDictionary dictionary)
    {
        super(Integer.MAX_VALUE, bitWidth, in);
        this.dictionary = dictionary;
    }

    @Override
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

            int numEntriesToFill = Math.min(remainingToCopy, currentCount);
            int endIndex = destinationIndex + numEntriesToFill;
            switch (mode) {
                case RLE: {
                    final int rleValue = currentValue;
                    final int rleDictionaryValue = dictionary.decodeToInt(rleValue);
                    while (destinationIndex < endIndex) {
                        values[destinationIndex++] = rleDictionaryValue;
                    }
                    break;
                }
                case PACKED: {
                    final int[] localCurrentBuffer = currentBuffer;
                    final IntegerDictionary localDictionary = dictionary;
                    for (int sourceIndex = currentBuffer.length - currentCount; destinationIndex < endIndex; sourceIndex++) {
                        int dictionaryValue = localDictionary.decodeToInt(localCurrentBuffer[sourceIndex]);
                        values[destinationIndex++] = dictionaryValue;
                    }
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + mode);
            }

            currentCount -= numEntriesToFill;
            remainingToCopy -= numEntriesToFill;
        }
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
