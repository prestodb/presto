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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.Type;

import io.airlift.slice.Slice;

import java.util.Iterator;
import java.util.List;

public class SubColumnBlock implements Block
{
    private final Block block;
    private final Block leafBlock;
    private final List<ColumnHandleReference> references;

    public SubColumnBlock(Block block, ColumnHandleReference reference)
    {
        this.references = reference.getHirarchies();
        this.block = block;

        Block current = block;
        BlockIterator iterator = new BlockIterator(block, 0);
        while (iterator.hasNext()) {
            current = iterator.next();
        }
        this.leafBlock = current;
    }

    public int getSliceLength(int position)
    {
        return leafBlock.getSliceLength(getPosition(position));
    }

    public byte getByte(int position, int offset)
    {
        return leafBlock.getByte(getPosition(position), offset);
    }

    public short getShort(int position, int offset)
    {
        return leafBlock.getShort(getPosition(position), offset);
    }

    public int getInt(int position, int offset)
    {
        return leafBlock.getInt(getPosition(position), offset);
    }

    public long getLong(int position, int offset)
    {
        return leafBlock.getLong(getPosition(position), offset);
    }

    public Slice getSlice(int position, int offset, int length)
    {
        return leafBlock.getSlice(getPosition(position), offset, length);
    }

    public <T> T getObject(int position, Class<T> clazz)
    {
        return leafBlock.getObject(getPosition(position), clazz);
    }

    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return leafBlock.bytesEqual(getPosition(position), offset, otherSlice, otherOffset, length);
    }

    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return leafBlock.bytesCompare(getPosition(position), offset, length, otherSlice, otherOffset, otherLength);
    }

    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        leafBlock.writeBytesTo(getPosition(position), offset, length, blockBuilder);
    }

    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
       return leafBlock.equals(getPosition(position), offset, otherBlock, otherPosition, otherOffset, length);
    }

    public long hash(int position, int offset, int length)
    {
        return leafBlock.hash(getPosition(position), offset, length);
    }

    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return leafBlock.compareTo(getPosition(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        Type type = getLeafType();

        Block current = block;
        BlockIterator iterator = new BlockIterator(block, position);
        while (iterator.hasNext()) {
            if (current.isNull(iterator.getPosition())) {
                break;
            }
            current = iterator.next();
        }

        if (current.isNull(iterator.getPosition())) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(current, iterator.getPosition(), blockBuilder);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        Type type = getLeafType();
        BlockBuilder resultBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1);

        Block current = block;
        BlockIterator iterator = new BlockIterator(block, position);
        while (iterator.hasNext()) {
            if (current.isNull(iterator.getPosition())) {
                resultBuilder.appendNull();
                return resultBuilder.build();
            }
            current = iterator.next();
        }
        type.appendTo(current, iterator.getPosition(), resultBuilder);
        return resultBuilder.build();
    }

    @Override
    public int getPositionCount()
    {
        return block.getPositionCount();
    }

    @Override
    public int getSizeInBytes()
    {
        return block.getSizeInBytes();
    }

    @Override
    public int getRegionSizeInBytes(int position, int length)
    {
        return block.getRegionSizeInBytes(position, length);
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return block.getRetainedSizeInBytes();
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return block.getEncoding();
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        Type type = getLeafType();
        BlockBuilder resultBuilder = type.createBlockBuilder(new BlockBuilderStatus(), positions.size());

        for (int position : positions) {
            Block current = block;
            BlockIterator iterator = new BlockIterator(block, position);
            while (iterator.hasNext()) {
                if (current.isNull(iterator.getPosition())) {
                    break;
                }
                current = iterator.next();
            }

            if (current.isNull(iterator.getPosition())) {
                resultBuilder.appendNull();
            }
            else {
                type.appendTo(current, iterator.getPosition(), resultBuilder);
            }
        }

        return resultBuilder.build();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        // if no ancestor is null within the region, get continuous region from leaf block
        // otherwise, the region is fragmented since null values from ancestors, use copyRegion then
        boolean ancestorNull = false;
        Block current = null;

        for (int offset = 0; offset < length && !ancestorNull; offset++) {
            int currentPosition = positionOffset + offset;
            current = block;

            for (int i = 0; i < references.size() - 2 && !current.isNull(currentPosition); i++) {
                ArrayBlock arrayBlock = (ArrayBlock) current;
                currentPosition = arrayBlock.getOffsets()[currentPosition];
                ColumnHandleReference reference = references.get(i);
                if (reference.arrayLength() > 0) {
                    currentPosition /= reference.arrayLength();
                }
                Block child = ((InterleavedBlock) arrayBlock.getValues()).getBlock(references.get(i + 1).arrayIndex());
                current = (child instanceof LazyBlock) ? ((LazyBlock) child).getBlock() : child;
            }

            if (current.isNull(currentPosition)) {
                ancestorNull = true;
            }
        }

        return (ancestorNull) ? copyRegion(positionOffset, length) : ((current != null) ? leafBlock.getRegion(positionOffset, length) : null);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        Type type = getLeafType();
        BlockBuilder resultBuilder = type.createBlockBuilder(new BlockBuilderStatus(), length);

        int remaining = length;
        while (remaining-- >= 0) {
            Block current = block;
            BlockIterator iterator = new BlockIterator(block, ++position);
            while (iterator.hasNext()) {
                if (current.isNull(iterator.getPosition())) {
                    break;
                }
                current = iterator.next();
            }

            if (current.isNull(iterator.getPosition())) {
                resultBuilder.appendNull();
            }
            else {
                type.appendTo(current, iterator.getPosition(), resultBuilder);
            }
        }

        return resultBuilder.build();
    }

    @Override
    public boolean isNull(int position)
    {
        Block current = block;
        BlockIterator iterator = new BlockIterator(block, position);
        while (iterator.hasNext()) {
            if (current.isNull(iterator.getPosition())) {
                return true;
            }
            current = iterator.next();
        }
        return current.isNull(iterator.getPosition());
    }

    private int getPosition(int position)
    {
        BlockIterator iterator = new BlockIterator(block, position);
        while (iterator.hasNext()) {
            iterator.next();
        }
        return iterator.getPosition();
    }

    public class BlockIterator implements Iterator<Block>
    {
        private Block current;
        private int index = 0;
        private int currentPosition;

        public BlockIterator(Block current, int currentPosition)
        {
            this.currentPosition = currentPosition;
            this.current = current;
        }

        public boolean hasNext()
        {
            if (index < references.size() - 1) {
                ArrayBlock arrayBlock = (ArrayBlock) current;
                currentPosition = arrayBlock.getOffsets()[currentPosition];
                ColumnHandleReference reference = references.get(index);
                if (reference.arrayLength() > 0) {
                    currentPosition /= reference.arrayLength();
                }
                Block child = ((InterleavedBlock) arrayBlock.getValues()).getBlock(references.get(index + 1).arrayIndex());
                current = (child instanceof LazyBlock) ? ((LazyBlock) child).getBlock() : child;
                index++;
                return true;
            }
            return false;
        }

        public int getPosition()
        {
            return currentPosition;
        }

        public Block next()
        {
            return current;
        }
    }

    private Type getLeafType()
    {
        return references.get(references.size() - 1).getType();
    }

    public interface ColumnHandleReference
    {
        List<ColumnHandleReference> getHirarchies();
        Type getType();
        int ordinal();
        int arrayLength();
        int arrayIndex();
    }
}
