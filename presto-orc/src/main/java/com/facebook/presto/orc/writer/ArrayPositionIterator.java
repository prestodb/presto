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

import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;

/**
 * Stores individual positions in an array list.
 * Intended to be used by the MapFlatColumnWriter.
 */
public class ArrayPositionIterator
        implements PositionIterator
{
    private final IntList positions = new IntArrayList();
    private int cursor;
    private boolean inUse;

    @Override
    public int next()
    {
        inUse = true;
        checkPositionIndex(cursor, positions.size());
        return positions.getInt(cursor++);
    }

    @Override
    public int getPositionCount()
    {
        return positions.size();
    }

    @Override
    public void reset()
    {
        inUse = false;
        cursor = 0;
    }

    @Override
    public void clear()
    {
        reset();
        positions.clear();
    }

    public void add(int position)
    {
        checkState(!inUse, "Cannot modify iterator while it is in use");
        positions.add(position);
    }
}
