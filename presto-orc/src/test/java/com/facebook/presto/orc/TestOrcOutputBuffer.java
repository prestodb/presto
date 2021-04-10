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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.CompressionParameters;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.Slices.wrappedBuffer;
import static org.testng.Assert.assertEquals;

public class TestOrcOutputBuffer
{
    @Test
    public void testWriteHugeByteChucks()
    {
        int size = 1024 * 1024;
        byte[] largeByteArray = new byte[size];
        Arrays.fill(largeByteArray, (byte) 0xA);
        CompressionParameters compressionParameters = new CompressionParameters(CompressionKind.NONE, OptionalInt.empty(), 256 * 1024);
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(compressionParameters, Optional.empty());

        DynamicSliceOutput output = new DynamicSliceOutput(size);
        sliceOutput.writeBytes(largeByteArray, 10, size - 10);
        assertEquals(sliceOutput.writeDataTo(output), size - 10);
        assertEquals(output.slice(), wrappedBuffer(largeByteArray, 10, size - 10));

        sliceOutput.reset();
        output.reset();
        sliceOutput.writeBytes(wrappedBuffer(largeByteArray), 100, size - 100);
        assertEquals(sliceOutput.writeDataTo(output), size - 100);
        assertEquals(output.slice(), wrappedBuffer(largeByteArray, 100, size - 100));
    }

    @Test
    public void testGrowCapacity()
    {
        byte[] largeByteArray = new byte[4096];
        CompressionParameters compressionParameters = new CompressionParameters(CompressionKind.NONE, OptionalInt.empty(), 3000);
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(compressionParameters, Optional.empty());

        // write some data that can fit the initial capacity = 256
        sliceOutput.writeBytes(largeByteArray, 0, 200);
        assertEquals(sliceOutput.getBufferCapacity(), 256);

        // write some more data to exceed the capacity = 256; the capacity will double
        sliceOutput.writeBytes(largeByteArray, 0, 200);
        assertEquals(sliceOutput.getBufferCapacity(), 512);

        // write a lot more data to exceed twice the capacity = 512 X 2; the capacity will be the required data size
        sliceOutput.writeBytes(largeByteArray, 0, 1200);
        assertEquals(sliceOutput.getBufferCapacity(), 1200);

        // write some more data to double the capacity again
        sliceOutput.writeBytes(largeByteArray, 0, 2000);
        assertEquals(sliceOutput.getBufferCapacity(), 2400);

        // make the buffer to reach the max buffer capacity
        sliceOutput.writeBytes(largeByteArray, 0, 2500);
        assertEquals(sliceOutput.getBufferCapacity(), 3000);

        // make sure we didn't miss anything
        DynamicSliceOutput output = new DynamicSliceOutput(6000);
        sliceOutput.close();
        assertEquals(sliceOutput.writeDataTo(output), 200 + 200 + 1200 + 2000 + 2500);
    }
}
