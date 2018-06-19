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
package com.facebook.presto.operator.project;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SelectedPositions
{
    private final boolean isList;
    private final int[] positions;
    private final int offset;
    private final int size;

    public static SelectedPositions empty()
    {
        return new SelectedPositions(false, new int[0], 0, 0);
    }

    public static SelectedPositions positionsList(int[] positions, int offset, int size)
    {
        return new SelectedPositions(true, positions, offset, size);
    }

    public static SelectedPositions positionsRange(int offset, int size)
    {
        return new SelectedPositions(false, new int[0], offset, size);
    }

    private SelectedPositions(boolean isList, int[] positions, int offset, int size)
    {
        this.isList = isList;
        this.positions = requireNonNull(positions, "positions is null");
        this.offset = offset;
        this.size = size;

        checkArgument(offset >= 0, "offset is negative");
        checkArgument(size >= 0, "size is negative");
        if (isList) {
            checkPositionIndexes(offset, offset + size, positions.length);
        }
    }

    public boolean isList()
    {
        return isList;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int[] getPositions()
    {
        checkState(isList, "SelectedPositions is a range");
        return positions;
    }

    public int getOffset()
    {
        return offset;
    }

    public int size()
    {
        return size;
    }

    public int getSelectedPosition(int position)
    {
        checkArgument(position >= 0 && position < size, "invalid position");
        if (isList) {
            return positions[offset + position];
        }
        return offset + position;
    }

    public Page selectPositions(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < blocks.length; channel++) {
            blocks[channel] = selectPositions(page.getBlock(channel));
        }
        return new Page(size, blocks);
    }

    public Block selectPositions(Block block)
    {
        if (isList) {
            return block.getPositions(positions, offset, size);
        }
        return block.getRegion(offset, size);
    }

    public SelectedPositions subRange(int start, int end)
    {
        checkPositionIndexes(start, end, size);

        int newOffset = this.offset + start;
        int newLength = end - start;
        return new SelectedPositions(isList, positions, newOffset, newLength);
    }
}
