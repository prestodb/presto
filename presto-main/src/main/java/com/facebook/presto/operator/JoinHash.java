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

import com.facebook.presto.block.BlockCursor;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.HashCommon;

import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class JoinHash
{
    private final PagesIndex pagesIndex;
    private final int[] hashChannels;

    private final int mask;
    private final int[] key;
    private final int[] positionLinks;

    public JoinHash(PagesIndex pagesIndex, List<Integer> hashChannels, OperatorContext operatorContext)
    {
        this.pagesIndex = pagesIndex;
        this.hashChannels = Ints.toArray(hashChannels);

        // reserve memory for the arrays
        int positionCount = pagesIndex.getPositionCount();
        int hashSize = HashCommon.arraySize(positionCount, 0.75f);
        operatorContext.reserveMemory(sizeOfIntArray(hashSize) + sizeOfIntArray(positionCount));

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        this.positionLinks = new int[positionCount];
        Arrays.fill(positionLinks, -1);

        // index pages
        for (int position = 0; position < positionCount; position++) {
            add(position);
        }
    }

    private void add(int position)
    {
        int pos = (murmurHash3(hashPosition(position) ^ mask)) & mask;

        while (key[pos] != -1) {
            int currentKey = key[pos];
            if (positionEqualsPosition(currentKey, position)) {
                key[pos] = position;

                // link the new position to the old position
                positionLinks[position] = currentKey;

                return;
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }

        // not found
        key[pos] = position;
    }

    public int getChannelCount()
    {
        return pagesIndex.getTypes().size();
    }

    public int getJoinPosition(BlockCursor... cursors)
    {
        int pos = (murmurHash3(hashCursor(cursors) ^ mask)) & mask;

        while (key[pos] != -1) {
            if (positionEqualsCurrentRow(key[pos], cursors)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    public int getNextJoinPosition(int currentPosition)
    {
        return positionLinks[currentPosition];
    }

    public void appendTo(int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int channel = 0; channel < getChannelCount(); channel++) {
            pagesIndex.appendTo(channel, position, pageBuilder.getBlockBuilder(outputChannelOffset + channel));
        }
    }

    private int hashCursor(BlockCursor... cursors)
    {
        int result = 0;
        for (BlockCursor cursor : cursors) {
            result = 31 * result + cursor.calculateHashCode();
        }
        return result;
    }

    private int hashPosition(int position)
    {
        return pagesIndex.hashCode(hashChannels, position);
    }

    private boolean positionEqualsCurrentRow(int position, BlockCursor... cursors)
    {
        return pagesIndex.equals(hashChannels, position, cursors);
    }

    private boolean positionEqualsPosition(int leftPosition, int rightPosition)
    {
        return pagesIndex.equals(hashChannels, leftPosition, rightPosition);
    }
}
