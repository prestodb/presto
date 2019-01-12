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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;

final class EmptyLookupSource
        implements LookupSource
{
    @Override
    public int getChannelCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return 0;
    }

    @Override
    public long getJoinPositionCount()
    {
        return 0;
    }

    @Override
    public long joinPositionWithinPartition(long joinPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        return -1;
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        return -1;
    }

    @Override
    public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        return false;
    }

    @Override
    public boolean isEmpty()
    {
        return true;
    }

    @Override
    public void close() {}
}
