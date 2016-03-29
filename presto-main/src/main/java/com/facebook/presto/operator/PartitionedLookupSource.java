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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;
import java.util.List;

import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class PartitionedLookupSource
        implements LookupSource
{
    private final LookupSource[] lookupSources;
    private final HashGenerator hashGenerator;
    private final int partitionMask;

    public PartitionedLookupSource(List<? extends LookupSource> lookupSources, List<Type> hashChannelTypes)
    {
        this.lookupSources = lookupSources.toArray(new LookupSource[lookupSources.size()]);

        // this generator is only used for getJoinPosition without a rawHash and in this case
        // the hash channels are always packed in a page without extra columns
        int[] hashChannels = new int[hashChannelTypes.size()];
        for (int i = 0; i < hashChannels.length; i++) {
            hashChannels[i] = i;
        }
        this.hashGenerator = new InterpretedHashGenerator(hashChannelTypes, hashChannels);

        this.partitionMask = lookupSources.size() - 1;
    }

    @Override
    public int getChannelCount()
    {
        return lookupSources[0].getChannelCount();
    }

    @Override
    public int getJoinPositionCount()
    {
        throw new UnsupportedOperationException("Parallel hash can not be used in a RIGHT or FULL outer join");
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return Arrays.stream(lookupSources).mapToLong(LookupSource::getInMemorySizeInBytes).sum();
    }

    @Override
    public long getJoinPosition(int position, Page page)
    {
        return getJoinPosition(position, page, hashGenerator.hashPosition(position, page));
    }

    @Override
    public long getJoinPosition(int position, Page page, long rawHash)
    {
        int partition = (int) murmurHash3(rawHash) & partitionMask;
        LookupSource lookupSource = lookupSources[partition];
        long joinPosition = lookupSource.getJoinPosition(position, page, rawHash);
        if (joinPosition < 0) {
            return joinPosition;
        }
        return encodePartitionedJoinPosition(partition, joinPosition);
    }

    @Override
    public long getNextJoinPosition(long partitionedJoinPosition)
    {
        int partition = (int) (partitionedJoinPosition & partitionMask);
        long joinPosition = partitionedJoinPosition >>> lookupSources.length;
        LookupSource lookupSource = lookupSources[partition];
        long nextJoinPosition = lookupSource.getNextJoinPosition(joinPosition);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }
        return encodePartitionedJoinPosition(partition, nextJoinPosition);
    }

    @Override
    public void appendTo(long partitionedJoinPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int partition = (int) (partitionedJoinPosition & partitionMask);
        long joinPosition = partitionedJoinPosition >>> lookupSources.length;
        LookupSource lookupSource = lookupSources[partition];
        lookupSource.appendTo(joinPosition, pageBuilder, outputChannelOffset);
    }

    @Override
    public void close()
    {
        for (LookupSource lookupSource : lookupSources) {
            lookupSource.close();
        }
    }

    private long encodePartitionedJoinPosition(int partition, long joinPosition)
    {
        return (joinPosition << lookupSources.length) | (partition);
    }
}
