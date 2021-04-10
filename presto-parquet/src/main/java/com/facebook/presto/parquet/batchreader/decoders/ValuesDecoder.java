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
package com.facebook.presto.parquet.batchreader.decoders;

import java.io.IOException;

public interface ValuesDecoder
{
    interface Int32ValuesDecoder
            extends ValuesDecoder
    {
        void readNext(int[] values, int offset, int length)
                throws IOException;

        void skip(int length)
                throws IOException;
    }

    interface BinaryValuesDecoder
            extends ValuesDecoder
    {
        ValueBuffer readNext(int length)
                throws IOException;

        int readIntoBuffer(byte[] byteBuffer, int bufferIndex, int[] offsets, int offsetIndex, ValueBuffer valueBuffer);

        void skip(int length)
                throws IOException;

        interface ValueBuffer
        {
            int getBufferSize();
        }
    }

    interface Int64ValuesDecoder
            extends ValuesDecoder
    {
        void readNext(long[] values, int offset, int length)
                throws IOException;

        void skip(int length)
                throws IOException;
    }

    interface Int64TimestampMicrosValuesDecoder
            extends ValuesDecoder
    {
        void readNext(long[] values, int offset, int length)
                throws IOException;

        void skip(int length)
                throws IOException;
    }

    interface TimestampValuesDecoder
            extends ValuesDecoder
    {
        void readNext(long[] values, int offset, int length)
                throws IOException;

        void skip(int length)
                throws IOException;
    }

    interface BooleanValuesDecoder
            extends ValuesDecoder
    {
        void readNext(byte[] values, int offset, int length);

        void skip(int length);
    }
}
