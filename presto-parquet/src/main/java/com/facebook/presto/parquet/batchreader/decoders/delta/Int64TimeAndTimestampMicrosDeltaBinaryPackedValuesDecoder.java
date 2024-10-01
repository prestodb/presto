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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64TimeAndTimestampMicrosValuesDecoder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Note: this is not an optimized values decoder. It makes use of the existing Parquet decoder. Given that this type encoding
 * is not a common one, just use the existing one provided by Parquet library and add a wrapper around it that satisfies the
 * {@link Int64ValuesDecoder} interface.
 */
public class Int64TimeAndTimestampMicrosDeltaBinaryPackedValuesDecoder
        implements Int64TimeAndTimestampMicrosValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int64TimeAndTimestampMicrosDeltaBinaryPackedValuesDecoder.class).instanceSize();

    private final DeltaBinaryPackingValuesReader innerReader;

    public Int64TimeAndTimestampMicrosDeltaBinaryPackedValuesDecoder(int valueCount, ByteBufferInputStream bufferInputStream)
            throws IOException
    {
        innerReader = new DeltaBinaryPackingValuesReader();
        innerReader.initFromPage(valueCount, bufferInputStream);
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        int endOffset = offset + length;
        for (int i = offset; i < endOffset; i++) {
            values[i] = MICROSECONDS.toMillis(innerReader.readLong());
        }
    }

    @Override
    public void skip(int length)
    {
        while (length > 0) {
            innerReader.skip();
            length--;
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // Not counting innerReader since it's in another library.
        return INSTANCE_SIZE;
    }
}
