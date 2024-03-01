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

import com.facebook.presto.orc.OrcLocalMemoryContext;

import javax.annotation.concurrent.NotThreadSafe;

import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class SharedBuffer
{
    private static final byte[] EMPTY_BUFFER = new byte[0];

    private final OrcLocalMemoryContext bufferMemoryUsage;

    private byte[] buffer = EMPTY_BUFFER;

    public SharedBuffer(OrcLocalMemoryContext bufferMemoryUsage)
    {
        this.bufferMemoryUsage = requireNonNull(bufferMemoryUsage, "bufferMemoryUsage is null");
    }

    public byte[] get()
    {
        return buffer;
    }

    public void ensureCapacity(int size)
    {
        if (buffer.length < size) {
            buffer = new byte[size];
            bufferMemoryUsage.setBytes(size);
        }
    }
}
