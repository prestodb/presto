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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.UuidValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.BinaryBatchDictionary;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Objects.requireNonNull;

public class UuidRLEDictionaryValuesDecoder
        extends BaseRLEBitPackedDecoder
        implements UuidValuesDecoder
{
    private final BinaryRLEDictionaryValuesDecoder delegate;

    public UuidRLEDictionaryValuesDecoder(int bitWidth, InputStream inputStream, BinaryBatchDictionary dictionary)
    {
        super(Integer.MAX_VALUE, bitWidth, inputStream);
        requireNonNull(dictionary, "dictionary is null");
        delegate = new BinaryRLEDictionaryValuesDecoder(bitWidth, inputStream, dictionary);
    }

    @Override
    public void readNext(long[] values, int offset, int length)
            throws IOException
    {
        BinaryValuesDecoder.ValueBuffer valueBuffer = delegate.readNext(length);
        int bufferSize = valueBuffer.getBufferSize();
        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[bufferSize + 1];
        delegate.readIntoBuffer(byteBuffer, 0, offsets, 0, valueBuffer);

        for (int i = 0; i < length; i++) {
            int positionOffset = offsets[i];
            int positionLength = offsets[i + 1] - positionOffset;
            byte[] temp = new byte[positionLength];
            System.arraycopy(byteBuffer, positionOffset, temp, 0, positionLength);
            ByteBuffer buf = ByteBuffer.wrap(temp).order(BIG_ENDIAN);
            values[2 * (offset + i)] = buf.getLong(0);
            values[2 * (offset + i) + 1] = buf.getLong(SIZE_OF_LONG);
        }
    }

    @Override
    public void skip(int length)
            throws IOException
    {
        delegate.skip(length);
    }
}
