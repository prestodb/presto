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

import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public interface CompressionBufferPool
{
    byte[] checkOut(int length);

    void checkIn(byte[] buffer);

    long getRetainedBytes();

    @NotThreadSafe
    class LastUsedCompressionBufferPool
            implements CompressionBufferPool
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(LastUsedCompressionBufferPool.class).instanceSize();
        private byte[] lastUsed;

        @Override
        public byte[] checkOut(int length)
        {
            if (lastUsed == null || lastUsed.length < length) {
                lastUsed = null;
                return new byte[length];
            }
            byte[] returnValue = lastUsed;
            lastUsed = null;
            return returnValue;
        }

        @Override
        public void checkIn(byte[] buffer)
        {
            lastUsed = requireNonNull(buffer, "buffer is null");
        }

        @Override
        public long getRetainedBytes()
        {
            return INSTANCE_SIZE + sizeOf(lastUsed);
        }
    }
}
