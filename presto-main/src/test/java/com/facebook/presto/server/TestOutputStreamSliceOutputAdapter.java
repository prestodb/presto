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
package com.facebook.presto.server;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;

public class TestOutputStreamSliceOutputAdapter
{
    @Test
    public void testEncodingBoolean()
            throws Exception
    {
        assertEncoding(sliceOutput -> sliceOutput.writeBoolean(true),
                new byte[] {1});
        assertEncoding(sliceOutput -> sliceOutput.writeBoolean(false),
                new byte[] {0});
    }

    @Test
    public void testEncodingByte()
            throws Exception
    {
        assertEncoding(sliceOutput -> sliceOutput.writeByte(92),
                new byte[] {92});
        assertEncoding(sliceOutput -> sliceOutput.writeByte(156),
                new byte[] {-100});
        assertEncoding(sliceOutput -> sliceOutput.writeByte(-17),
                new byte[] {-17});
    }

    @Test
    public void testEncodingShort()
            throws Exception
    {
        assertEncoding(sliceOutput -> sliceOutput.writeShort(23661),
                new byte[] {109, 92});
        assertEncoding(sliceOutput -> sliceOutput.writeShort(40045),
                new byte[] {109, -100});
        assertEncoding(sliceOutput -> sliceOutput.writeShort(-27188),
                new byte[] {-52, -107});
    }

    @Test
    public void testEncodingInteger()
            throws Exception
    {
        assertEncoding(sliceOutput -> sliceOutput.writeInt(978017389),
                new byte[] {109, 92, 75, 58});
        assertEncoding(sliceOutput -> sliceOutput.writeInt(-7813904),
                new byte[] {-16, -60, -120, -1});
    }

    @Test
    public void testEncodingLong()
            throws Exception
    {
        assertEncoding(sliceOutput -> sliceOutput.writeLong(-1184314682315678611L),
                new byte[] {109, 92, 75, 58, 18, 120, -112, -17});
    }

    @Test
    public void testEncodingDouble()
            throws Exception
    {
        assertEncoding(sliceOutput -> sliceOutput.writeDouble(3.14),
                new byte[] {31, -123, -21, 81, -72, 30, 9, 64});
        assertEncoding(sliceOutput -> sliceOutput.writeDouble(Double.NaN),
                new byte[] {0, 0, 0, 0, 0, 0, -8, 127});
        assertEncoding(sliceOutput -> sliceOutput.writeDouble(Double.NEGATIVE_INFINITY),
                new byte[] {0, 0, 0, 0, 0, 0, -16, -1});
        assertEncoding(sliceOutput -> sliceOutput.writeDouble(Double.POSITIVE_INFINITY),
                new byte[] {0, 0, 0, 0, 0, 0, -16, 127});
    }

    @Test
    public void testEncodingBytes()
            throws Exception
    {
        byte[] data = new byte[18000];
        ThreadLocalRandom.current().nextBytes(data);

        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 0), Arrays.copyOfRange(data, 0, 0));
        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 3), Arrays.copyOfRange(data, 0, 3));
        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 370), Arrays.copyOfRange(data, 0, 370));
        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 4095), Arrays.copyOfRange(data, 0, 4095));
        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 4096), Arrays.copyOfRange(data, 0, 4096));
        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 12348), Arrays.copyOfRange(data, 0, 12348));
        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 16384), Arrays.copyOfRange(data, 0, 16384));
        assertEncoding(sliceOutput -> sliceOutput.write(data, 0, 18000), Arrays.copyOfRange(data, 0, 18000));
    }

    @Test
    public void testEncodingSlice()
            throws Exception
    {
        byte[] data = new byte[18000];
        ThreadLocalRandom.current().nextBytes(data);
        Slice slice = Slices.wrappedBuffer(data);

        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 0), Arrays.copyOfRange(data, 0, 0));
        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 3), Arrays.copyOfRange(data, 0, 3));
        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 370), Arrays.copyOfRange(data, 0, 370));
        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 4095), Arrays.copyOfRange(data, 0, 4095));
        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 4096), Arrays.copyOfRange(data, 0, 4096));
        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 12348), Arrays.copyOfRange(data, 0, 12348));
        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 16384), Arrays.copyOfRange(data, 0, 16384));
        assertEncoding(sliceOutput -> sliceOutput.writeBytes(slice, 0, 18000), Arrays.copyOfRange(data, 0, 18000));
    }

    @Test
    public void testWriteZero()
            throws Exception
    {
        assertEncoding(sliceOutput -> sliceOutput.writeZero(0), new byte[0]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(1), new byte[1]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(2), new byte[2]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(3), new byte[3]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(4), new byte[4]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(6), new byte[6]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(7), new byte[7]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(8), new byte[8]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(9), new byte[9]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(16), new byte[16]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(22), new byte[22]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(227), new byte[227]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(4227), new byte[4227]);
        assertEncoding(sliceOutput -> sliceOutput.writeZero(18349), new byte[18349]);
    }

    /**
     * Asserting different offsets of operations.
     */
    private void assertEncoding(Consumer<SliceOutput> operations, byte... expected)
            throws IOException
    {
        assertEncoding(operations, 0, expected);
        assertEncoding(operations, 1, expected);
        assertEncoding(operations, 2, expected);
        assertEncoding(operations, 3, expected);
        assertEncoding(operations, 4, expected);
        assertEncoding(operations, 7, expected);
        assertEncoding(operations, 8, expected);
        assertEncoding(operations, 16, expected);
        assertEncoding(operations, 511, expected);
        assertEncoding(operations, 12000, expected);
        assertEncoding(operations, 13000, expected);
        assertEncoding(operations, 16000, expected);
        assertEncoding(operations, 16380, expected);
        assertEncoding(operations, 16383, expected);
        assertEncoding(operations, 16384, expected);
        assertEncoding(operations, 18349, expected);
    }

    private void assertEncoding(Consumer<SliceOutput> operations, int offset, byte... output)
            throws IOException
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (SliceOutput sliceOutput = new OutputStreamSliceOutputAdapter(byteArrayOutputStream, 16384)) {
            sliceOutput.writeZero(offset);
            operations.accept(sliceOutput);
            assertEquals(sliceOutput.size(), offset + output.length);
        }

        byte[] expected = new byte[offset + output.length];
        System.arraycopy(output, 0, expected, offset, output.length);
        assertEquals(byteArrayOutputStream.toByteArray(), expected);
    }
}
