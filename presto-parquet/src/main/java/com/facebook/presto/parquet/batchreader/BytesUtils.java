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
package com.facebook.presto.parquet.batchreader;

public class BytesUtils
{
    private BytesUtils()
    {
    }

    public static final int getInt(byte[] byteBuffer, int offset)
    {
        int ch0 = byteBuffer[offset + 0] & 255;
        int ch1 = byteBuffer[offset + 1] & 255;
        int ch2 = byteBuffer[offset + 2] & 255;
        int ch3 = byteBuffer[offset + 3] & 255;

        return (ch3 << 24) + (ch2 << 16) + (ch1 << 8) + ch0;
    }

    public static final long getLong(byte[] byteBuffer, int offset)
    {
        int ch0 = byteBuffer[offset + 0];
        int ch1 = byteBuffer[offset + 1];
        int ch2 = byteBuffer[offset + 2];
        int ch3 = byteBuffer[offset + 3];
        int ch4 = byteBuffer[offset + 4];
        int ch5 = byteBuffer[offset + 5];
        int ch6 = byteBuffer[offset + 6];
        int ch7 = byteBuffer[offset + 7];

        return ((long) (ch7 & 255) << 56) +
                ((long) (ch6 & 255) << 48) +
                ((long) (ch5 & 255) << 40) +
                ((long) (ch4 & 255) << 32) +
                ((long) (ch3 & 255) << 24) +
                ((long) (ch2 & 255) << 16) +
                ((long) (ch1 & 255) << 8) +
                ((long) (ch0 & 255) << 0);
    }

    public static void unpack8Values(byte inByte, byte[] out, int outPos)
    {
        out[0 + outPos] = (byte) (inByte & 1);
        out[1 + outPos] = (byte) (inByte >> 1 & 1);
        out[2 + outPos] = (byte) (inByte >> 2 & 1);
        out[3 + outPos] = (byte) (inByte >> 3 & 1);
        out[4 + outPos] = (byte) (inByte >> 4 & 1);
        out[5 + outPos] = (byte) (inByte >> 5 & 1);
        out[6 + outPos] = (byte) (inByte >> 6 & 1);
        out[7 + outPos] = (byte) (inByte >> 7 & 1);
    }
}
