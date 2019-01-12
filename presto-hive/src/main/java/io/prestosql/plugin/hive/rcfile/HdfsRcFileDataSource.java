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
package io.prestosql.plugin.hive.rcfile;

import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.rcfile.RcFileDataSource;
import io.prestosql.rcfile.RcFileDataSourceId;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HdfsRcFileDataSource
        implements RcFileDataSource
{
    private final FSDataInputStream inputStream;
    private final String path;
    private final long size;
    private final FileFormatDataSourceStats stats;
    private long readTimeNanos;
    private long readBytes;

    public HdfsRcFileDataSource(String path, FSDataInputStream inputStream, long size, FileFormatDataSourceStats stats)
    {
        this.path = requireNonNull(path, "path is null");
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.size = size;
        checkArgument(size >= 0, "size is negative");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public RcFileDataSourceId getId()
    {
        return new RcFileDataSourceId(path);
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    public long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        inputStream.readFully(position, buffer, bufferOffset, bufferLength);

        long readDuration = System.nanoTime() - start;
        stats.readDataBytesPerSecond(bufferLength, readDuration);

        readTimeNanos += readDuration;
        readBytes += bufferLength;
    }

    @Override
    public String toString()
    {
        return path;
    }
}
