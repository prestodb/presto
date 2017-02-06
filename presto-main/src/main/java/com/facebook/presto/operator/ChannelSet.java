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
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.type.UnknownType.UNKNOWN;

public class ChannelSet
{
    private final GroupByHash hash;
    private final boolean containsNull;
    private final int[] hashChannels;

    public ChannelSet(GroupByHash hash, boolean containsNull, int[] hashChannels)
    {
        this.hash = hash;
        this.containsNull = containsNull;
        this.hashChannels = hashChannels;
    }

    public Type getType()
    {
        return hash.getTypes().get(0);
    }

    public long getEstimatedSizeInBytes()
    {
        return hash.getEstimatedSize();
    }

    public int size()
    {
        return hash.getGroupCount();
    }

    public boolean containsNull()
    {
        return containsNull;
    }

    public boolean contains(int position, Page page)
    {
        return hash.contains(position, page, hashChannels);
    }

    public static class ChannelSetBuilder
    {
        private static final int[] HASH_CHANNELS = { 0 };

        private final GroupByHash hash;
        private final OperatorContext operatorContext;
        private final Page nullBlockPage;

        public ChannelSetBuilder(Type type, Optional<Integer> hashChannel, int expectedPositions, OperatorContext operatorContext, JoinCompiler joinCompiler)
        {
            List<Type> types = ImmutableList.of(type);
            this.hash = createGroupByHash(operatorContext.getSession(), types, HASH_CHANNELS, hashChannel, expectedPositions, joinCompiler);
            this.operatorContext = operatorContext;
            this.nullBlockPage = new Page(type.createBlockBuilder(new BlockBuilderStatus(), 1, UNKNOWN.getFixedSize()).appendNull().build());
        }

        public ChannelSet build()
        {
            return new ChannelSet(hash, hash.contains(0, nullBlockPage, HASH_CHANNELS), HASH_CHANNELS);
        }

        public long getEstimatedSize()
        {
            return hash.getEstimatedSize();
        }

        public int size()
        {
            return hash.getGroupCount();
        }

        public void addPage(Page page)
        {
            hash.addPage(page);

            if (operatorContext != null) {
                operatorContext.setMemoryReservation(hash.getEstimatedSize());
            }
        }
    }
}
