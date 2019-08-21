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
package com.facebook.presto.parquet;

import static java.util.Objects.requireNonNull;

public abstract class AbstractParquetDataSource
        implements ParquetDataSource
{
    private final ParquetDataSourceId id;
    private long readTimeNanos;
    private long readBytes;

    public AbstractParquetDataSource(ParquetDataSourceId id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public ParquetDataSourceId getId()
    {
        return id;
    }

    @Override
    public final long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public final void readFully(long position, byte[] buffer)
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public final void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        readBytes += bufferLength;

        long start = System.nanoTime();
        readInternal(position, buffer, bufferOffset, bufferLength);
        long currentReadTimeNanos = System.nanoTime() - start;

        readTimeNanos += currentReadTimeNanos;
    }

    protected abstract void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength);
}
