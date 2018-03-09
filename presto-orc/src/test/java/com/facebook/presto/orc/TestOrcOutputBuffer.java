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
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.Arrays;

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
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(CompressionKind.NONE, 256 * 1024);

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
}
