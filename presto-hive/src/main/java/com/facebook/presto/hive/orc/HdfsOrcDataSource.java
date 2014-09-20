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
package com.facebook.presto.hive.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HdfsOrcDataSource
        implements OrcDataSource
{
    private final Path path;
    private final FileSystem fileSystem;
    private final FSDataInputStream inputStream;
    private long readTimeNanos;

    public HdfsOrcDataSource(Path path, FileSystem fileSystem)
            throws IOException
    {
        this(path, fileSystem, fileSystem.open(path));
    }

    public HdfsOrcDataSource(Path path, FileSystem fileSystem, FSDataInputStream inputStream)
    {
        this.path = path;
        this.fileSystem = fileSystem;
        this.inputStream = checkNotNull(inputStream, "inputStream is null");
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getSize()
            throws IOException
    {
        return fileSystem.getFileStatus(path).getLen();
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        inputStream.readFully(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
    }

    @Override
    public List<StripeSlice> readFully(long position, List<DiskRange> diskRanges)
            throws IOException
    {
        checkArgument(position >= 0, "position is negative");
        checkNotNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableList.of();
        }

        // merge ranges
        DiskRange fullRange = diskRanges.get(0);
        for (DiskRange diskRange : diskRanges) {
            fullRange = fullRange.mergeWith(diskRange);
        }

        // read full range in one request
        byte[] buffer = new byte[fullRange.getLength()];
        readFully(position + fullRange.getOffset(), buffer);
        return ImmutableList.of(new StripeSlice(Slices.wrappedBuffer(buffer), fullRange.getOffset()));
    }

    @Override
    public String toString()
    {
        return path.toString();
    }
}
