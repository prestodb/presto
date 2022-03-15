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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MockParquetDataSource
        implements ParquetDataSource
{
    private final ParquetDataSourceId id;
    private final long estimatedSize;
    private final FSDataInputStream inputStream;
    private long readTimeNanos;
    private long readBytes;

    public MockParquetDataSource(
            ParquetDataSourceId id,
            long estimatedSize,
            FSDataInputStream inputStream)
    {
        this.id = requireNonNull(id, "id is null");
        this.estimatedSize = estimatedSize;
        this.inputStream = inputStream;
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
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    public final void readFully(long position, byte[] buffer)
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        readBytes += bufferLength;

        long start = System.nanoTime();
        try {
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (Exception e) {
            throw new RuntimeException("Error reading from %s " + id + " at position " + position);
        }
        long currentReadTimeNanos = System.nanoTime() - start;

        readTimeNanos += currentReadTimeNanos;
    }

    @Override
    public Optional<ColumnIndex> readColumnIndex(ColumnChunkMetaData column)
            throws IOException
    {
        throw new NotSupportedException("Not supported");
    }

    @Override
    public Optional<OffsetIndex> readOffsetIndex(ColumnChunkMetaData column)
            throws IOException
    {
        throw new NotSupportedException("Not supported");
    }
}
