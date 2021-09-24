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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDictionaryAggregationEnabled;
import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.google.common.base.Preconditions.checkState;

public class MarkDistinctHash
{
    private final GroupByHash groupByHash;
    private long nextDistinctId;

    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, JoinCompiler joinCompiler, UpdateMemory updateMemory)
    {
        this(session, types, channels, hashChannel, 10_000, joinCompiler, updateMemory);
    }

    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, int expectedDistinctValues, JoinCompiler joinCompiler, UpdateMemory updateMemory)
    {
        this.groupByHash = createGroupByHash(types, channels, hashChannel, expectedDistinctValues, isDictionaryAggregationEnabled(session), joinCompiler, updateMemory);
    }

    public long getEstimatedSize()
    {
        return groupByHash.getEstimatedSize();
    }

    public Work<Block> markDistinctRows(Page page)
    {
        return new TransformWork<>(groupByHash.getGroupIds(page), this::processNextGroupIds);
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.getCapacity();
    }

    public List<Page> getDistinctPages()
    {
        return groupByHash.getBufferedPages();
    }

    private Block processNextGroupIds(GroupByIdBlock ids)
    {
        int positions = ids.getPositionCount();
        if (positions > 1) {
            // must have > 1 positions to benefit from using a RunLengthEncoded block
            if (nextDistinctId == ids.getGroupCount()) {
                // no new distinct positions
                return new RunLengthEncodedBlock(BooleanType.createBlockForSingleNonNullValue(false), positions);
            }
            if (nextDistinctId + positions == ids.getGroupCount()) {
                // all positions are distinct
                nextDistinctId = ids.getGroupCount();
                return new RunLengthEncodedBlock(BooleanType.createBlockForSingleNonNullValue(true), positions);
            }
        }
        byte[] distinctMask = new byte[positions];
        for (int position = 0; position < distinctMask.length; position++) {
            if (ids.getGroupId(position) == nextDistinctId) {
                distinctMask[position] = 1;
                nextDistinctId++;
            }
            else {
                distinctMask[position] = 0;
            }
        }
        checkState(nextDistinctId == ids.getGroupCount());
        return BooleanType.wrapByteArrayAsBooleanBlockWithoutNulls(distinctMask);
    }
}
