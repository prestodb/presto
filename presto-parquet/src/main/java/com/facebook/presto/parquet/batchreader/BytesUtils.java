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
}
