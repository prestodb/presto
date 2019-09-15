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
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.BinaryValuesDecoder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Note: this is not an optimized values decoder. It makes use of the existing Parquet decoder. Given that this type encoding
 * is not a common one, just use the existing one provided by Parquet library and add a wrapper around it that satisfies the
 * {@link BinaryValuesDecoder} interface.
 */
public class BinaryDeltaValuesDecoder
        implements BinaryValuesDecoder
{
    private final ValuesReader innerReader;

    public BinaryDeltaValuesDecoder(ParquetEncoding encoding, int valueCount, ByteBufferInputStream bufferInputStream)
            throws IOException
    {
        if (encoding == ParquetEncoding.DELTA_BYTE_ARRAY) {
            innerReader = new DeltaByteArrayReader();
        }
        else if (encoding == ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY) {
            innerReader = new DeltaLengthByteArrayValuesReader();
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding: " + encoding);
        }

        innerReader.initFromPage(valueCount, bufferInputStream);
    }

    @Override
    public ReadChunk readNext(int length)
            throws IOException
    {
        Binary[] values = new Binary[length];
        int bufferSize = 0;
        for (int i = 0; i < length; i++) {
            Binary value = innerReader.readBytes();
            values[i] = value;
            bufferSize += value.length();
        }

        return new ReadChunkDelta(values, bufferSize);
    }

    @Override
    public int readIntoBuffer(byte[] byteBuffer, int bufferIdx, int[] offsets, int offsetIdx, ReadChunk readChunk)
    {
        checkArgument(byteBuffer.length - bufferIdx >= readChunk.getBufferSize(), "not enough space in the input buffer");

        ReadChunkDelta readChunkDelta = (ReadChunkDelta) readChunk;

        final Binary[] values = readChunkDelta.values;
        for (int i = 0; i < values.length; i++) {
            Binary value = values[i];

            offsets[offsetIdx++] = bufferIdx;
            byte[] valueBytes = value.getBytes();
            System.arraycopy(valueBytes, 0, byteBuffer, bufferIdx, valueBytes.length);
            bufferIdx += valueBytes.length;
        }
        offsets[offsetIdx] = bufferIdx;

        return bufferIdx;
    }

    @Override
    public void skip(int length)
            throws IOException
    {
        while (length > 0) {
            innerReader.skip();
            length--;
        }
    }

    private static class ReadChunkDelta
            implements ReadChunk
    {
        private final Binary[] values;
        private final int bufferSize;

        public ReadChunkDelta(Binary[] values, int bufferSize)
        {
            this.values = values;
            this.bufferSize = bufferSize;
        }

        @Override
        public int getBufferSize()
        {
            return bufferSize;
        }
    }
}
