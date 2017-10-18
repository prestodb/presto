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
package com.facebook.presto.operator.index;

import com.facebook.presto.Session;
import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.operator.index.IndexedData.UNLOADED_INDEX_KEY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class UnloadedIndexPageSet
        implements PageSet
{
    private final List<Page> pages;
    private final List<Type> types;

    public UnloadedIndexPageSet(
            Session session,
            IndexSnapshot existingSnapshot,
            Set<Integer> channelsForDistinct,
            List<Type> types,
            List<UpdateRequest> requests,
            JoinCompiler joinCompiler)
    {
        requireNonNull(existingSnapshot, "existingSnapshot is null");
        requireNonNull(requests, "requests is null");
        this.types = requireNonNull(types, "types is null");
        pages = buildPages(session, existingSnapshot, channelsForDistinct, types, requests, joinCompiler);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    private List<Page> buildPages(Session session,
            IndexSnapshot existingSnapshot,
            Set<Integer> channelsForDistinct,
            List<Type> types,
            List<UpdateRequest> requests,
            JoinCompiler joinCompiler)
    {
        int[] distinctChannels = Ints.toArray(channelsForDistinct);
        int[] normalizedDistinctChannels = new int[distinctChannels.length];
        List<Type> distinctChannelTypes = new ArrayList<>(distinctChannels.length);
        for (int i = 0; i < distinctChannels.length; i++) {
            normalizedDistinctChannels[i] = i;
            distinctChannelTypes.add(types.get(distinctChannels[i]));
        }

        ImmutableList.Builder<Page> builder = ImmutableList.builder();
        GroupByHash groupByHash = createGroupByHash(session, distinctChannelTypes, normalizedDistinctChannels, Optional.empty(), 10_000, joinCompiler);
        for (UpdateRequest request : requests) {
            Page page = request.getPage();
            Block[] blocks = page.getBlocks();

            Block[] distinctBlocks = new Block[distinctChannels.length];
            for (int i = 0; i < distinctBlocks.length; i++) {
                distinctBlocks[i] = blocks[distinctChannels[i]];
            }

            // Move through the positions while advancing the cursors in lockstep
            Work<GroupByIdBlock> work = groupByHash.getGroupIds(new Page(distinctBlocks));
            boolean done = work.process();
            // TODO: this class does not yield wrt memory limit; enable it
            verify(done);
            GroupByIdBlock groupIds = work.getResult();
            int positionCount = blocks[0].getPositionCount();
            long nextDistinctId = -1;
            checkArgument(groupIds.getGroupCount() <= Integer.MAX_VALUE);
            IntList positions = new IntArrayList((int) groupIds.getGroupCount());
            for (int position = 0; position < positionCount; position++) {
                // We are reading ahead in the cursors, so we need to filter any nulls since they can not join
                if (!containsNullValue(position, blocks)) {
                    // Only include the key if it is not already in the index
                    if (existingSnapshot.getJoinPosition(position, page) == UNLOADED_INDEX_KEY) {
                        // Only add the position if we have not seen this tuple before (based on the distinct channels)
                        long groupId = groupIds.getGroupId(position);
                        if (nextDistinctId < groupId) {
                            nextDistinctId = groupId;
                            positions.add(position);
                        }
                    }
                }
            }

            if (!positions.isEmpty()) {
                builder.add(page.mask(positions.toIntArray()));
            }
        }

        return builder.build();
    }

    private static boolean containsNullValue(int position, Block... blocks)
    {
        for (Block block : blocks) {
            if (block.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Page> getPages()
    {
        return pages;
    }
}
