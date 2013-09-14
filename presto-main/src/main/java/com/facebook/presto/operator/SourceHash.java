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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import io.airlift.slice.Slice;

public class SourceHash
{
    private final ChannelHash channelHash;
    private final PagesIndex pagesIndex;
    private final int channelCount;

    public SourceHash(ChannelHash channelHash, PagesIndex pagesIndex)
    {
        this.channelHash = channelHash;
        this.pagesIndex = pagesIndex;
        this.channelCount = pagesIndex.getTupleInfos().size();
    }

    public int getChannelCount()
    {
        return channelCount;
    }

    public void setProbeSlice(Slice slice)
    {
        channelHash.setLookupSlice(slice);
    }

    public int getJoinPosition(BlockCursor cursor)
    {
        return channelHash.get(cursor);
    }

    public int getNextJoinPosition(int joinPosition)
    {
        return channelHash.getNextPosition(joinPosition);
    }

    public void appendTupleTo(int channel, int position, BlockBuilder blockBuilder)
    {
        pagesIndex.appendTupleTo(channel, position, blockBuilder);
    }
}
