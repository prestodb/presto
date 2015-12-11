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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

// keeps the top (smallest) N elements in a stream.
// we are not really implementing a heap here, because we can't maintain
// the min or provide poll(); instead, we keep an INTERNAL max heap whose top is the N'th element seen so far
// poll() is not needed by context this class is used in: we only need to keep topN
// NOT thread safe!
public class MultiChannelTopNAccumulator
{
    private static final DataSize OVERHEAD_PER_VALUE = new DataSize(100, DataSize.Unit.BYTE); // for estimating in-memory size. This is a completely arbitrary number
    // if we want to keep top N elements, we keep a buffer of 5*N before compacting
    private static final int BUFFER_HEAD_ROOM = 5;

    private final List<? extends Type> sourceTypes;
    private final List<Integer> sortChannels;
    private final List<? extends Type> sortTypes;
    private final List<SortOrder> sortOrders;
    private BlockBuilder[] blocks;
    private PriorityQueue<UpdatableInteger> pointers;
    private Comparator<UpdatableInteger> comparator;

    private final int n;
    private final int bufferCapacity;

    // sourceTypes and sortTypes are usually different, for example in a 10-column row, we only
    // use 2 columns for sorting
    public MultiChannelTopNAccumulator(
            List<? extends Type> sourceTypes,
            int n,
            List<Integer> sortChannels,
            List<? extends Type> sortTypes,
            List<SortOrder> sortOrders
    )
    {
        this.sourceTypes = sourceTypes;
        this.sortChannels = sortChannels;
        this.sortTypes = sortTypes;
        this.sortOrders = sortOrders;
        this.n = n;
        bufferCapacity = n * BUFFER_HEAD_ROOM;
        comparator = new Comparator<UpdatableInteger>()
        {
            // maxHeap, so reverse the wanted order
            @Override
            public int compare(UpdatableInteger posA, UpdatableInteger posB)
            {
                return -lexicoCompare(posA.getValue(), posB.getValue());
            }
        };
        initBlockBuilders();
        pointers = new PriorityQueue<UpdatableInteger>(n, comparator);
    }

    /**
     * adds the pos'th row from sourceBlocks into our heap --- if it fits among the new top N
     *
     * @param sourceBlocks
     * @param pos
     * @return change in our storage size
     */
    public int add(Block[] sourceBlocks, int pos)
    {
        if (pointers.size() == n
                && lexicoCompare(sourceBlocks, pos, blocks, pointers.peek().getValue()) >= 0) {
            return 0;
        }
        int newIdx = appendRow(sourceBlocks, pos);
        int sizeDelta = 0;
        if (pointers.size() == n) {
            int purgedIdx = pointers.poll().getValue();
            sizeDelta -= sizeOfBlocks(blocks, purgedIdx);
        }

        pointers.add(new UpdatableInteger(newIdx));
        sizeDelta += sizeOfBlocks(blocks, newIdx);

        return sizeDelta;
    }

    // once this is called, internal block storage is changed
    public BlocksSnapShot flushContent()
    {
        // since the pointers heap keep the max row first but we
        // need to export content by smallest row first (remember we are asked to keep and
        // export the first N, IN ORDER), we need to reverse the indices.
        // we have to poll() one by one instead of new ArrayList<>(pointers)
        // since the latter obeys "no particular order"
        List<Integer> reverseIdx = new ArrayList<Integer>();
        while (pointers.size() > 0) {
            int idx = pointers.poll().getValue();
            reverseIdx.add(idx);
        }

        Collections.reverse(reverseIdx);
        Block[] copyBlocks = blocks;
        initBlockBuilders();
        pointers.clear();
        // avoid excessive buffer copy, just ship the out-of-order buffer, with pointers
        return new BlocksSnapShot(copyBlocks, reverseIdx, sourceTypes);
    }

    private void initBlockBuilders()
    {
        blocks = new BlockBuilder[sourceTypes.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = sourceTypes.get(i).createBlockBuilder(new BlockBuilderStatus(), bufferCapacity);
        }
    }

    // copy the content of the pos'th row from the source blocks into our own
    // buffer blocks, resize/compact if necessary
    int appendRow(Block[] sourceBlocks, int pos)
    {
        assert (blocks.length > 0);
        assert (sourceBlocks.length == blocks.length);

        compactIfNecessary();

        for (int i = 0; i < blocks.length; i++) {
            sourceTypes.get(i).appendTo(sourceBlocks[i], pos, blocks[i]);
        }
        return blocks[0].getPositionCount() - 1;
    }

    void compactIfNecessary()
    {
        if (blocks[0].getPositionCount() == bufferCapacity) {
            doCompact();
        }
    }

    void doCompact()
    {
        //sort of a copy garbage collector
        BlockBuilder[] oldBlocks = blocks;
        initBlockBuilders();

        // running size of newly-allocated buffer
        int cnt = 0;
        for (UpdatableInteger idx : pointers) {
            int oldIdx = idx.getValue();
            for (int i = 0; i < sourceTypes.size(); i++) {
                Type type = sourceTypes.get(i);
                type.appendTo(oldBlocks[i], oldIdx, blocks[i]);
            }
            idx.setValue(cnt);
            cnt++;
        }
    }

    int lexicoCompare(int posA, int posB)
    {
        return lexicoCompare(blocks, posA, blocks, posB);
    }

    // compare according to the natural order specified in sortOrder
    // lexicographic ordering
    int lexicoCompare(
            Block[] blocksA,
            int posA,
            Block[] blocksB,
            int posB)
    {
        for (int i = 0; i < sortTypes.size(); i++) {
            Type type = sortTypes.get(i);
            int channel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);

            // compare the right value to the left block but negate the result since we are evaluating in the opposite order
            int compare = sortOrder.compareBlockValue(type, blocksA[channel], posA, blocksB[channel], posB);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    static long sizeOfBlocks(Block[] blocks, int position)
    {
        int result = 0;
        for (Block block : blocks) {
            //FIXME just to demonstrate what we want to achieve here, but apparently
            // we want to avoid this value copy
            Block[] row = getValues(position, blocks);
            result += sizeOfRow(row);
        }
        return result;
    }

    static long sizeOfRow(Block[] row)
    {
        long size = OVERHEAD_PER_VALUE.toBytes();
        for (Block value : row) {
            size += value.getRetainedSizeInBytes();
        }
        return size;
    }

    private static Block[] getValues(int position, Block[] blocks)
    {
        Block[] row = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            row[i] = blocks[i].getSingleValueBlock(position);
        }
        return row;
    }

    /**
     * sort of an iterator, to avoid having to dump to another Block[] when
     * such dumping is not strictly needed. it's assumed that users mostly call
     * popRow().
     */
    static class BlocksSnapShot
    {
        private Block[] blocks;
        private int pos;
        private List<Integer> indices;
        private final List<? extends Type> sourceTypes;

        public BlocksSnapShot(Block[] blocks, List<Integer> indices, List<? extends Type> sourceTypes)
        {
            this.blocks = blocks;
            this.indices = indices;
            pos = 0;
            this.sourceTypes = sourceTypes;
        }

        public boolean hasNext()
        {
            return pos < indices.size();
        }

        public void popRow(BlockBuilder[] outBlocks)
        {
            for (int i = 0; i < outBlocks.length; i++) {
                sourceTypes.get(i).appendTo(blocks[i], indices.get(pos), outBlocks[i]);
            }
            pos++;
        }

        // export cleanly sorted, dense, blocks
        Block[] toBlocks()
        {
            BlockBuilder[] result = new BlockBuilder[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                result[i] = sourceTypes.get(i).createBlockBuilder(new BlockBuilderStatus(), indices.size());
            }

            for (int i = 0; i < indices.size(); i++) {
                popRow(result);
            }

            return result;
        }
    }

    static class UpdatableInteger
    {
        private int value;

        public UpdatableInteger(int n)
        {
            this.value = n;
        }

        public int getValue()
        {
            return value;
        }

        public void setValue(int n)
        {
            value = n;
        }
    }
}
