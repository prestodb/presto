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
package com.facebook.presto.hive.orc.stream;

import java.io.IOException;
import java.io.InputStream;

public class BooleanStream
{
    private final ByteStream byteStream;
    private byte data;
    private int bitsInData;

    public BooleanStream(InputStream byteStream)
            throws IOException
    {
        this.byteStream = new ByteStream(byteStream);
    }

    private void readByte()
            throws IOException
    {
        data = byteStream.next();
        bitsInData = 8;
    }

    public boolean nextBit()
            throws IOException
    {
        // read more data if necessary
        if (bitsInData == 0) {
            readByte();
        }

        // read bit
        boolean result = (data & 0b1000_00000) != 0;

        // mark bit consumed
        data <<= 1;
        bitsInData--;

        return result;
    }

    public void skip(int items)
            throws IOException
    {
        if (bitsInData >= items) {
            bitsInData -= items;
        }
        else {
            items -= bitsInData;

            byteStream.skip(items >>> 3);
            items = items & 0b111;

            readByte();
            data <<= items;
            bitsInData -= items;
        }
    }

    public int countBitsSet(int items)
            throws IOException
    {
        int count = 0;

        // count buffered data
        if (items > bitsInData && bitsInData > 0) {
            count += bitCount(data);
            items -= bitsInData;
            bitsInData = 0;
        }

        // count whole bytes
        while (items > 8) {
            count += bitCount(byteStream.next());
            items -= 8;
        }

        // count remaining bits
        for (int i = 0; i < items; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            if ((data & 0b1000_00000) != 0) {
                count++;
            }

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }

        return count;
    }

    /**
     * Sets the vector element to true if the bit is set.
     */
    public void getSetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            vector[i] = (data & 0b1000_00000) != 0;

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }
    }

    /**
     * Sets the vector element to true if the bit is set, skipping the null values.
     */
    public void getSetBits(int batchSize, boolean[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            if (!isNull[i]) {
                // read more data if necessary
                if (bitsInData == 0) {
                    readByte();
                }

                // read bit
                vector[i] = (data & 0b1000_00000) != 0;

                // mark bit consumed
                data <<= 1;
                bitsInData--;
            }
        }
    }

    /**
     * Sets the vector element to true if the bit is not set.
     */
    public void getUnsetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            vector[i] = (data & 0b1000_00000) == 0;

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }
    }

    private static int bitCount(byte data)
    {
        return Integer.bitCount(data & 0xFF);
    }
}
