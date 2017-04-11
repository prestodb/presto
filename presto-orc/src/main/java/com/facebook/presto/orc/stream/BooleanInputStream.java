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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

@SuppressWarnings("NarrowingCompoundAssignment")
public class BooleanInputStream
        implements ValueInputStream<BooleanStreamCheckpoint>
{
    private static final int HIGH_BIT_MASK = 0b1000_0000;
    private final ByteInputStream byteStream;
    private byte data;
    private int bitsInData;

    public BooleanInputStream(OrcInputStream byteStream)
    {
        this.byteStream = new ByteInputStream(byteStream);
    }

    private void readByte()
            throws IOException
    {
        checkState(bitsInData == 0);
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
        boolean result = (data & HIGH_BIT_MASK) != 0;

        // mark bit consumed
        data <<= 1;
        bitsInData--;

        return result;
    }

    @Override
    public Class<BooleanStreamCheckpoint> getCheckpointType()
    {
        return BooleanStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(BooleanStreamCheckpoint checkpoint)
            throws IOException
    {
        byteStream.seekToCheckpoint(checkpoint.getByteStreamCheckpoint());
        bitsInData = 0;
        skip(checkpoint.getOffset());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        if (bitsInData >= items) {
            data <<= items;
            bitsInData -= items;
        }
        else {
            items -= bitsInData;
            bitsInData = 0;

            byteStream.skip(items >>> 3);
            items &= 0b111;

            if (items != 0) {
                readByte();
                data <<= items;
                bitsInData -= items;
            }
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
            if ((data & HIGH_BIT_MASK) != 0) {
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
            vector[i] = (data & HIGH_BIT_MASK) != 0;

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
                vector[i] = (data & HIGH_BIT_MASK) != 0;

                // mark bit consumed
                data <<= 1;
                bitsInData--;
            }
        }
    }

    /**
     * Sets the vector element to true if the bit is set.
     */
    public void getSetBits(Type type, int batchSize, BlockBuilder builder)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            type.writeBoolean(builder, (data & HIGH_BIT_MASK) != 0);

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }
    }

    /**
     * Sets the vector element to true if the bit is set, skipping the null values.
     */
    public void getSetBits(Type type, int batchSize, BlockBuilder builder, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            if (isNull[i]) {
                builder.appendNull();
            }
            else {
                // read more data if necessary
                if (bitsInData == 0) {
                    readByte();
                }

                // read bit
                type.writeBoolean(builder, (data & HIGH_BIT_MASK) != 0);

                // mark bit consumed
                data <<= 1;
                bitsInData--;
            }
        }
    }

    /**
     * Sets the vector element to true if the bit is not set.
     */
    public int getUnsetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        int count = 0;
        for (int i = 0; i < batchSize; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            vector[i] = (data & HIGH_BIT_MASK) == 0;
            if (vector[i]) {
                count++;
            }

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }
        return count;
    }

    private static int bitCount(byte data)
    {
        return Integer.bitCount(data & 0xFF);
    }
}
