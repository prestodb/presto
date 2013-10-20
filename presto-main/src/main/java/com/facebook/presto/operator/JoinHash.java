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
import it.unimi.dsi.fastutil.HashCommon;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public abstract class JoinHash
{
    private final int channelCount;
    private final int mask;
    private final int[] key;
    private final int[] positionLinks;

    public JoinHash(int channelCount, int positionCount, OperatorContext operatorContext)
    {
        this.channelCount = channelCount;

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(positionCount, 0.75f);
        operatorContext.reserveMemory(sizeOfIntArray(hashSize) + sizeOfIntArray(positionCount));

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        this.positionLinks = new int[positionCount];
        Arrays.fill(positionLinks, -1);
    }

    protected final void buildHash(int positionCount)
    {
        // index pages
        for (int position = 0; position < positionCount; position++) {
            add(position);
        }
    }

    public final int getChannelCount()
    {
        return channelCount;
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

    public final int getJoinPosition(BlockCursor... cursors)
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

    public final int getNextJoinPosition(int currentPosition)
    {
        return positionLinks[currentPosition];
    }

    public abstract void appendTo(int position, PageBuilder pageBuilder, int outputChannelOffset);

    protected abstract int hashCursor(BlockCursor... cursors);

    protected abstract int hashPosition(int position);

    protected abstract boolean positionEqualsCurrentRow(int position, BlockCursor... cursors);

    protected abstract boolean positionEqualsPosition(int leftPosition, int rightPosition);
}
