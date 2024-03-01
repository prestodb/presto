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
package com.facebook.presto.orc.writer;

import com.facebook.presto.orc.writer.CompressionBufferPool.LastUsedCompressionBufferPool;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

public class TestCompressionBufferPool
{
    @Test
    public void testBufferReuse()
    {
        CompressionBufferPool bufferPool = new LastUsedCompressionBufferPool();
        byte[] buffer1 = bufferPool.checkOut(0);
        verifyBuffer(buffer1, 0);
        bufferPool.checkIn(buffer1);

        byte[] buffer2 = bufferPool.checkOut(0);
        assertSame(buffer1, buffer2);
        // Do not checkIn buffer2, but ask for another buffer

        byte[] buffer3 = bufferPool.checkOut(0);
        assertNotSame(buffer1, buffer3);
        verifyBuffer(buffer3, 0);

        bufferPool.checkIn(buffer3);
    }

    @Test
    public void testLargeBuffer()
    {
        CompressionBufferPool bufferPool = new LastUsedCompressionBufferPool();
        byte[] buffer1 = bufferPool.checkOut(1000);
        verifyBuffer(buffer1, 1000);
        bufferPool.checkIn(buffer1);

        byte[] buffer2 = bufferPool.checkOut(500);
        assertSame(buffer1, buffer2);
        bufferPool.checkIn(buffer2);

        byte[] buffer3 = bufferPool.checkOut(2000);
        verifyBuffer(buffer3, 2000);
    }

    private void verifyBuffer(byte[] buffer, int expectedLength)
    {
        assertNotNull(buffer);
        assertEquals(buffer.length, expectedLength);
    }
}
