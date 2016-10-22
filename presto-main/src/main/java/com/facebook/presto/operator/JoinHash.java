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
import com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class JoinHash
        implements LookupSource
{
    private final PagesHash pagesHash;

    // we unwrap Optional<JoinFilterFunction> to actual verifier or null in constructor for performance reasons
    // we do quick check for `filterFunction == null` in `getNextJoinPositionFrom` to avoid calls to applyFilterFunction
    @Nullable
    private final JoinFilterFunction filterFunction;

    public JoinHash(PagesHash pagesHash, Optional<JoinFilterFunction> filterFunction)
    {
        this.pagesHash = requireNonNull(pagesHash, "pagesHash is null");
        this.filterFunction = requireNonNull(filterFunction, "filterFunction can not be null").orElse(null);
    }

    @Override
    public final int getChannelCount()
    {
        return pagesHash.getChannelCount();
    }

    @Override
    public int getJoinPositionCount()
    {
        return pagesHash.getPositionCount();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return pagesHash.getInMemorySizeInBytes();
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        int addressIndex = pagesHash.getAddressIndex(position, hashChannelsPage, allChannelsPage);
        if (addressIndex == -1) {
            return -1;
        }
        return getNextJoinPositionFrom(addressIndex, position, allChannelsPage);
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int addressIndex = pagesHash.getAddressIndex(position, hashChannelsPage, allChannelsPage, rawHash);
        if (addressIndex == -1) {
            return -1;
        }
        return getNextJoinPositionFrom(addressIndex, position, allChannelsPage);
    }

    @Override
    public final long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        int nextAddressIndex = pagesHash.getNextAddressIndex(Ints.checkedCast(currentJoinPosition));
        return getNextJoinPositionFrom(nextAddressIndex, probePosition, allProbeChannelsPage);
    }

    private int getNextJoinPositionFrom(int currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        while (filterFunction != null && currentJoinPosition != -1 && !filterFunction.filter((currentJoinPosition), probePosition, allProbeChannelsPage)) {
            currentJoinPosition = pagesHash.getNextAddressIndex(currentJoinPosition);
        }
        return currentJoinPosition;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        pagesHash.appendTo(Ints.checkedCast(position), pageBuilder, outputChannelOffset);
    }
}
