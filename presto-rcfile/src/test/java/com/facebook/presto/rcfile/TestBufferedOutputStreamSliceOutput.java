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
package com.facebook.presto.rcfile;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import static com.facebook.airlift.testing.Assertions.assertLessThanOrEqual;
import static org.testng.Assert.assertEquals;

public class TestBufferedOutputStreamSliceOutput
{
    @Test
    public void testWriteBytes()
            throws Exception
    {
        // fill up some input bytes
        int length = 65536;
        byte[] inputArray = new byte[length];
        for (int i = 0; i < length; i++) {
            inputArray[i] = (byte) (i % 128);
        }

        // pick some offsets to make the inputs into different chunks
        int[] offsets = {0, 100, 545, 1024, 2049, 2050, 2051, 2151, 10480, 20042, 20100, 40001, 65536};

        // check byte array version
        MockOutputStream byteOutputStream = new MockOutputStream(length);
        BufferedOutputStreamSliceOutput output = new BufferedOutputStreamSliceOutput(byteOutputStream);
        for (int i = 0; i < offsets.length - 1; i++) {
            output.writeBytes(inputArray, offsets[i], offsets[i + 1] - offsets[i]);
        }
        // ignore the last flush size check
        output.flush();
        assertEquals(byteOutputStream.toByteArray(), inputArray);
        byteOutputStream.close();

        // check slice version
        byteOutputStream = new MockOutputStream(length);
        Slice inputSlice = Slices.wrappedBuffer(inputArray);
        output = new BufferedOutputStreamSliceOutput(byteOutputStream);
        for (int i = 0; i < offsets.length - 1; i++) {
            output.writeBytes(inputSlice, offsets[i], offsets[i + 1] - offsets[i]);
        }
        // ignore the last flush size check
        output.flush();
        assertEquals(byteOutputStream.toByteArray(), inputArray);
        byteOutputStream.close();
    }

    private class MockOutputStream
            extends ByteArrayOutputStream
    {
        public MockOutputStream(int length)
        {
            super(length);
        }

        @Override
        public void write(byte[] source, int sourceIndex, int length)
        {
            assertLessThanOrEqual(length, 4096);
            super.write(source, sourceIndex, length);
        }
    }
}
