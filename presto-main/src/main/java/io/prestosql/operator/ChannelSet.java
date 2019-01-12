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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.prestosql.operator.GroupByHash.createGroupByHash;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

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

    public boolean isEmpty()
    {
        return size() == 0;
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
        private static final int[] HASH_CHANNELS = {0};

        private final GroupByHash hash;
        private final Page nullBlockPage;
        private final OperatorContext operatorContext;
        private final LocalMemoryContext localMemoryContext;

        public ChannelSetBuilder(Type type, Optional<Integer> hashChannel, int expectedPositions, OperatorContext operatorContext, JoinCompiler joinCompiler)
        {
            List<Type> types = ImmutableList.of(type);
            this.hash = createGroupByHash(
                    types,
                    HASH_CHANNELS,
                    hashChannel,
                    expectedPositions,
                    isDictionaryAggregationEnabled(operatorContext.getSession()),
                    joinCompiler,
                    this::updateMemoryReservation);
            this.nullBlockPage = new Page(type.createBlockBuilder(null, 1, UNKNOWN.getFixedSize()).appendNull().build());
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.localMemoryContext = operatorContext.localUserMemoryContext();
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

        public Work<?> addPage(Page page)
        {
            // Just add the page to the pending work, which will be processed later.
            return hash.addPage(page);
        }

        public boolean updateMemoryReservation()
        {
            // If memory is not available, once we return, this operator will be blocked until memory is available.
            localMemoryContext.setBytes(hash.getEstimatedSize());

            // If memory is not available, inform the caller that we cannot proceed for allocation.
            return operatorContext.isWaitingForMemory().isDone();
        }

        @VisibleForTesting
        public int getCapacity()
        {
            return hash.getCapacity();
        }
    }
}
