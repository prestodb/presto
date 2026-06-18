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

import java.util.Map;

public class NoopOrcDataSource
        implements OrcDataSource
{
    public static final NoopOrcDataSource INSTANCE = new NoopOrcDataSource();

    @Override
    public OrcDataSourceId getId()
    {
        return new OrcDataSourceId("fake");
    }

    @Override
    public long getReadBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReadTimeNanos()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer)
    {
        // do nothing
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        // do nothing
    }

    @Override
    public <K> Map<K, OrcDataSourceInput> readFully(Map<K, DiskRange> diskRanges)
    {
        throw new UnsupportedOperationException();
    }
}
