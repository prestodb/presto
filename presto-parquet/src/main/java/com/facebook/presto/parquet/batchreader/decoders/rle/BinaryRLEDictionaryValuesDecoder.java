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

    public BinaryRLEDictionaryValuesDecoder(int bitWidth, InputStream in, BinaryBatchDictionary dictionary)
    {
        super(Integer.MAX_VALUE, bitWidth, in);
        this.dictionary = dictionary;
    }

    @Override
    public ReadChunk readNext(int length)
            throws IOException
    {
        int[] dictIds = new int[length];
        int destIndex = 0;
        int bufferSize = 0;
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
                    final int rleValueLength = dictionary.getLength(rleValue);
                    while (destIndex < endIndex) {
                        dictIds[destIndex++] = rleValue;
                    }
                    bufferSize += (rleValueLength * numEntriesToFill);
                    break;
                }
                case PACKED: {
                    final int[] localCurrentBuffer = this.currentBuffer;
                    final BinaryBatchDictionary localDictionary = this.dictionary;
                    for (int srcIndex = this.currentBuffer.length - this.currentCount; destIndex < endIndex; srcIndex++, destIndex++) {
                        int dictId = localCurrentBuffer[srcIndex];
                        dictIds[destIndex] = dictId;
                        bufferSize += localDictionary.getLength(dictId);
                    }
                    break;
                }
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }

            this.currentCount -= numEntriesToFill;
            remainingToCopy -= numEntriesToFill;
        }

        checkState(remainingToCopy == 0, "Invalid read size request");

        return new ReadChunkRLE(bufferSize, dictIds);
    }

    @Override
    public int readIntoBuffer(byte[] byteBuffer, int bufferIdx, int[] offsets, int offsetIdx, ReadChunk readChunk)
    {
        checkArgument(byteBuffer.length - bufferIdx >= readChunk.getBufferSize(), "not enough space in the input buffer");

        ReadChunkRLE readChunkRLE = (ReadChunkRLE) readChunk;

        final int[] dictionaryIds = readChunkRLE.getDictionaryIds();
        final int numEntries = dictionaryIds.length;

        for (int i = 0; i < numEntries; i++) {
            offsets[offsetIdx++] = bufferIdx;
            bufferIdx += dictionary.copyTo(byteBuffer, bufferIdx, dictionaryIds[i]);
        }
        offsets[offsetIdx] = bufferIdx;

        return bufferIdx;
    }

    @Override
    public void skip(int length)
            throws IOException
    {
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

        checkState(remaining == 0, "Invalid read size request");
    }

    public static class ReadChunkRLE
            implements ReadChunk
    {
        private final int bufferSize;
        private final int[] dictionaryIds;

        public ReadChunkRLE(int bufferSize, int[] dictionaryIds)
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
