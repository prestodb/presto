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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.ConcatenatedByteArrayInputStream;
import io.airlift.http.client.GatheringByteArrayInputStream;
import io.airlift.http.client.ResponseListener;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Math.min;

@ThreadSafe
class BufferingResponseListener
        implements ResponseListener
{
    private static final long BUFFER_MAX_BYTES = new DataSize(1, MEGABYTE).toBytes();
    private static final long BUFFER_MIN_BYTES = new DataSize(1, KILOBYTE).toBytes();

    @GuardedBy("this")
    private byte[] currentBuffer = new byte[0];
    @GuardedBy("this")
    private int currentBufferPosition;
    @GuardedBy("this")
    private List<byte[]> buffers = new ArrayList<>();
    @GuardedBy("this")
    private long size;
    private final ExchangeClientByteArrayAllocator byteArrayAllocator;

    public BufferingResponseListener(ExchangeClientByteArrayAllocator byteArrayAllocator)
    {
        this.byteArrayAllocator = byteArrayAllocator;
    }

    @Override
    public synchronized void onContent(ByteBuffer content)
    {
        int length = content.remaining();
        size += length;

        while (length > 0) {
            if (currentBufferPosition >= currentBuffer.length) {
                allocateCurrentBuffer();
            }
            int readLength = min(length, currentBuffer.length - currentBufferPosition);
            content.get(currentBuffer, currentBufferPosition, readLength);
            length -= readLength;
            currentBufferPosition += readLength;
        }
    }

    @Override
    public synchronized InputStream onComplete()
    {
        if (byteArrayAllocator != null) {
            return new ConcatenatedByteArrayInputStream(buffers, size, byteArrayAllocator.toPrestoAllocator());
        }
        return new GatheringByteArrayInputStream(buffers, size);
    }

    private synchronized void allocateCurrentBuffer()
    {
        checkState(currentBufferPosition >= currentBuffer.length, "there is still remaining space in currentBuffer");
        int size = (int) min(BUFFER_MAX_BYTES, max(2 * currentBuffer.length, BUFFER_MIN_BYTES));
        if (byteArrayAllocator != null) {
            currentBuffer = byteArrayAllocator.allocate(size);
        }
        else {
            currentBuffer = new byte[size];
        }
        buffers.add(currentBuffer);
        currentBufferPosition = 0;
    }
}
