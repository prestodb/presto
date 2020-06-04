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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.BinaryBatchDictionary;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class BinaryRLEDictionaryValuesDecoder
        extends BaseRLEBitPackedDecoder
        implements BinaryValuesDecoder
{
    private final BinaryBatchDictionary dictionary;

    public BinaryRLEDictionaryValuesDecoder(int bitWidth, InputStream inputStream, BinaryBatchDictionary dictionary)
    {
        super(Integer.MAX_VALUE, bitWidth, inputStream);
        this.dictionary = dictionary;
    }

    @Override
    public ValueBuffer readNext(int length)
            throws IOException
    {
        int[] dictionaries = new int[length];
        int destinationIndex = 0;
        int bufferSize = 0;
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
                    final int rleValueLength = dictionary.getLength(rleValue);
                    while (destinationIndex < endIndex) {
                        dictionaries[destinationIndex++] = rleValue;
                    }
                    bufferSize += (rleValueLength * numEntriesToFill);
                    break;
                }
                case PACKED: {
                    final int[] localBuffer = currentBuffer;
                    final BinaryBatchDictionary localDictionary = dictionary;
                    for (int srcIndex = currentBuffer.length - currentCount; destinationIndex < endIndex; srcIndex++, destinationIndex++) {
                        int dictionaryId = localBuffer[srcIndex];
                        dictionaries[destinationIndex] = dictionaryId;
                        bufferSize += localDictionary.getLength(dictionaryId);
                    }
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }
            currentCount -= numEntriesToFill;
            remainingToCopy -= numEntriesToFill;
        }

        checkState(remainingToCopy == 0, "Invalid read size request");
        return new RLEValueBuffer(bufferSize, dictionaries);
    }

    @Override
    public int readIntoBuffer(byte[] byteBuffer, int bufferIndex, int[] offsets, int offsetIndex, ValueBuffer valueBuffer)
    {
        checkArgument(byteBuffer.length - bufferIndex >= valueBuffer.getBufferSize(), "not enough space in the input buffer");

        RLEValueBuffer rleValueBuffer = (RLEValueBuffer) valueBuffer;
        final int[] dictionaryIds = rleValueBuffer.getDictionaryIds();
        final int numEntries = dictionaryIds.length;

        for (int i = 0; i < numEntries; i++) {
            offsets[offsetIndex++] = bufferIndex;
            bufferIndex += dictionary.copyTo(byteBuffer, bufferIndex, dictionaryIds[i]);
        }
        offsets[offsetIndex] = bufferIndex;
        return bufferIndex;
    }

    @Override
    public void skip(int length)
            throws IOException
    {
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
        checkState(remaining == 0, "Invalid read size request");
    }

    public static class RLEValueBuffer
            implements ValueBuffer
    {
        private final int bufferSize;
        private final int[] dictionaryIds;

        public RLEValueBuffer(int bufferSize, int[] dictionaryIds)
        {
            this.bufferSize = bufferSize;
            this.dictionaryIds = dictionaryIds;
        }

        @Override
        public int getBufferSize()
        {
            return bufferSize;
        }

        public int[] getDictionaryIds()
        {
            return dictionaryIds;
        }
    }
}
