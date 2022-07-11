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
package com.facebook.presto.orc.writer;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;

/**
 * Position iterator working with ranges.
 * Intended to be used by the ListColumnWriter.
 */
public class RangePositionIterator
        implements PositionIterator
{
    private final IntList positions = new IntArrayList();
    private final IntList lengths = new IntArrayList();
    private int cursor = -1;
    private int size;
    private boolean inUse;

    private int position;
    private int rangePositionsLeft;

    @Override
    public int next()
    {
        inUse=true;
        advanceToNextRangeIfNeeded();
        rangePositionsLeft--;
        return position++;
    }

    private void advanceToNextRangeIfNeeded()
    {
        if (rangePositionsLeft == 0) {
            cursor++;
            checkPositionIndex(cursor, positions.size());
            position = positions.getInt(cursor);
            rangePositionsLeft = lengths.getInt(cursor);
        }
    }

    @Override
    public int getPositionCount()
    {
        return size;
    }

    @Override
    public void reset()
    {
        inUse = false;
        cursor = -1;
        rangePositionsLeft = 0;
    }

    @Override
    public void clear()
    {
        reset();
        positions.clear();
        lengths.clear();
        size = 0;
    }

    // Add a range of positions: [position, position+1, ..., position+length].
    public void addRange(int position, int length)
    {
        checkArgument(position >= 0, "position is negative");
        checkArgument(length >= 0, "length is negative");
        checkState(!inUse, "Cannot modify iterator while it is in use");

        if (length > 0) {
            int positionsSize = positions.size();
            int index = positionsSize - 1;

            // attempt to merge ranges
            if (positionsSize > 0 && positions.getInt(index) + lengths.getInt(index) == position) {
                lengths.set(index, lengths.getInt(index) + length);
            }
            else {
                positions.add(position);
                lengths.add(length);
            }
            this.size += length;
        }
    }
}
