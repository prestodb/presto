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
package com.facebook.presto.orc.compression;

import com.facebook.presto.orc.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.orc.memory.LocalMemoryContext;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;

public abstract class Codec
    implements AutoCloseable
{
    private final LocalMemoryContext bufferMemoryUsage;

    private final int maxBufferSize;
    private byte[] buffer;

    public Codec(int maxBufferSize, AbstractAggregatedMemoryContext systemMemoryContext)
    {
        this.maxBufferSize = maxBufferSize;
        this.bufferMemoryUsage = systemMemoryContext.newLocalMemoryContext();
    }

    public abstract Slice decompress(Slice compressedSlice)
            throws IOException;

    @Override
    public void close()
    {
        buffer = null;
        bufferMemoryUsage.setBytes(0);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("maxBufferSize", maxBufferSize)
                .toString();
    }

    protected byte[] allocateOrGrowBuffer(int size, boolean copyExistingData)
    {
        if (buffer == null || buffer.length < size) {
            if (copyExistingData && buffer != null) {
                buffer = Arrays.copyOfRange(buffer, 0, Math.min(size, maxBufferSize));
            }
            else {
                buffer = new byte[Math.min(size, maxBufferSize)];
            }
        }
        bufferMemoryUsage.setBytes(buffer.length);
        return buffer;
    }
}
