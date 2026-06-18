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
package com.facebook.presto.parquet.batchreader.decoders.delta;

import com.facebook.presto.parquet.ParquetEncoding;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.ShortDecimalValuesDecoder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;

import static com.facebook.presto.parquet.ParquetTypeUtils.getShortDecimalValue;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BinaryShortDecimalDeltaValuesDecoder
        implements ShortDecimalValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BinaryShortDecimalDeltaValuesDecoder.class).instanceSize();

    private final BinaryDeltaValuesDecoder delegate;

    public BinaryShortDecimalDeltaValuesDecoder(ParquetEncoding encoding, int valueCount, ByteBufferInputStream bufferInputStream)
            throws IOException
    {
        requireNonNull(encoding, "encoding is null");
        requireNonNull(bufferInputStream, "bufferInputStream is null");
        delegate = new BinaryDeltaValuesDecoder(encoding, valueCount, bufferInputStream);
    }

    @Override
    public void readNext(long[] values, int offset, int length)
            throws IOException
    {
        BinaryValuesDecoder.ValueBuffer valueBuffer = delegate.readNext(length);
        int bufferSize = valueBuffer.getBufferSize();
        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[length + 1];
        delegate.readIntoBuffer(byteBuffer, 0, offsets, 0, valueBuffer);

        for (int i = 0; i < length; i++) {
            int positionOffset = offsets[i];
            int positionLength = offsets[i + 1] - positionOffset;
            if (positionLength > 8) {
                throw new ParquetDecodingException("Unable to read BINARY type decimal of size " + positionLength + " as a short decimal");
            }

            values[offset + i] = getShortDecimalValue(byteBuffer, positionOffset, positionLength);
        }
    }

    @Override
    public void skip(int length)
            throws IOException
    {
        checkArgument(length >= 0, "invalid length %s", length);
        delegate.skip(length);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
