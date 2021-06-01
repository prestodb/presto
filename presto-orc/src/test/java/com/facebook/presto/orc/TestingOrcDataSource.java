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

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

class TestingOrcDataSource
        implements OrcDataSource
{
    private final OrcDataSource delegate;
    private final List<DiskRange> readRanges = new ArrayList<>();
    private int readCount;
    private List<DiskRange> lastReadRanges;

    public TestingOrcDataSource(OrcDataSource delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public OrcDataSourceId getId()
    {
        return delegate.getId();
    }

    public int getReadCount()
    {
        return readCount;
    }

    public List<DiskRange> getLastReadRanges()
    {
        return lastReadRanges;
    }

    public List<DiskRange> getReadRanges()
    {
        return ImmutableList.copyOf(readRanges);
    }

    @Override
    public long getReadBytes()
    {
        return delegate.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public long getSize()
    {
        return delegate.getSize();
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readCount++;
        lastReadRanges = ImmutableList.of(new DiskRange(position, buffer.length));
        readRanges.addAll(lastReadRanges);
        delegate.readFully(position, buffer);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        readCount++;
        lastReadRanges = ImmutableList.of(new DiskRange(position, bufferLength));
        readRanges.addAll(lastReadRanges);
        delegate.readFully(position, buffer, bufferOffset, bufferLength);
    }

    @Override
    public <K> Map<K, OrcDataSourceInput> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        readCount += diskRanges.size();
        lastReadRanges = ImmutableList.copyOf(diskRanges.values());
        readRanges.addAll(lastReadRanges);
        return delegate.readFully(diskRanges);
    }
}
