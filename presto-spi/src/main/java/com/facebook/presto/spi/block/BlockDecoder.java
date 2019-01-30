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

import io.airlift.slice.Slice;

public class BlockDecoder
{
    public Block leafBlock;
    public long[] longs;
    public double[] doubles;
    public boolean[] valueIsNull;
    public Slice slice;
    public int[] offsets;
    public int[] rowNumberMap;
    int arrayOffset;
    public boolean isIdentityMap;
    boolean isMapOwned;
    IntArrayAllocator intArrayAllocator;
    static volatile int[] identityMap;

    public BlockDecoder()
    {
    }

    public BlockDecoder(IntArrayAllocator intArrayALlocator)
    {
        this.intArrayAllocator = intArrayAllocator;
    }


    public boolean[] getValueIsNull()
    {
        return valueIsNull;
    }

    public long[] getLongs()
    {
        return longs;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public int[] getOffsets()
    {
        return offsets;
    }

    public Block getLeafBlock()
    {
        return leafBlock;
    }

    public int[] getRowNumberMap()
    {
        return rowNumberMap;
    }

    public boolean isIdentityMap()
    {
        return this.isIdentityMap;
    }

    static int[] getIdentityMap(int size, int start, IntArrayAllocator intArrayAllocator)
    {
        if (start == 0) {
            int[] map = identityMap;
            if (map != null && map.length >= size) {
                return map;
            }
            map = new int[size + 100];
            for (int i = 0; i < map.length; ++i) {
                map[i] = i;
            }
            identityMap = map;
            return map;
        }
        int[] map = intArrayAllocator.getIntArray(size);
        for (int i = 0; i < size; ++i) {
            map[i] = i + start;
        }
        return map;
    }

    public void decodeBlock(Block block)
    {
        if (intArrayAllocator == null) {
            intArrayAllocator = new IntArrayAllocator();
        }
        decodeBlock(block, intArrayAllocator);
    }

    public void decodeBlock(Block block, IntArrayAllocator intArrayAllocator) {
        int positionCount = block.getPositionCount();
        isMapOwned = false;
        isIdentityMap = true;
        longs = null;
        slice = null;
        offsets = null;
        rowNumberMap = null;
        int[] map = null;
        for (;;) {
            if (block instanceof DictionaryBlock) {
                isIdentityMap = false;
                int[] ids = ((DictionaryBlock)block).getIdsArray();
                int offset = ((DictionaryBlock)block).getIdsOffset();
                if (map == null) {
                    map = ids;
                    if (offset != 0) {
                        int[] newMap = intArrayAllocator.getIntArray(positionCount);
                        System.arraycopy(map, offset, newMap, 0, positionCount);
                        isMapOwned = true;
                        map = newMap;
                    }
                }
                else {
                    if (!isMapOwned) {
                        int[] newMap = intArrayAllocator.getIntArray(positionCount);
                        for (int i = 0; i < positionCount; ++i) {
                            newMap[i] = ids[map[i] + offset];
                        }
                        isMapOwned = true;
                    }
                    else {
                        for (int i = 0; i < positionCount; ++i) {
                            map[i] = ids[map[i] + offset];
                        }
                    }
                }
                block = ((DictionaryBlock)block).getDictionary();
            }
            else if (block instanceof RunLengthEncodedBlock)
                {
                    if (map == null || !isMapOwned) {
                        map = intArrayAllocator.getIntArray(positionCount);
                        isMapOwned = true;
                        isIdentityMap = false;
                    }
                    for (int i = 0; i < positionCount; ++i) {
                        map[i] = 0;
                    }
                    block = ((RunLengthEncodedBlock)block).getValue();
                }
            else {
                leafBlock = block;
                block.getContents(this);
                if (arrayOffset != 0) {
                    if (isMapOwned) {
                        for (int i = 0; i < positionCount; i++) {
                            map[i] += arrayOffset;
                        }
                    }
                    else if (map == null) {
                        map = intArrayAllocator.getIntArray(positionCount);
                        for (int i = 0; i < positionCount; i++) {
                            map[i] = i + arrayOffset;
                        }
                    }
                    else {
                        int[] newMap = intArrayAllocator.getIntArray(positionCount);
                        System.arraycopy(map, 0, newMap, 0, positionCount);
                        for (int i = 0; i  < positionCount; i++) {
                            newMap[i] += arrayOffset;
                        }
                        map = newMap;
                    }
                    isMapOwned = true;
                    isIdentityMap = false;
                    rowNumberMap = map;
                }
                else {
                    if (map == null) {
                        isIdentityMap = true;
                        rowNumberMap = getIdentityMap(positionCount, 0, null);
                    }
                    else {
                        rowNumberMap = map;
                    }
                }
                return;
            }
        }
    }

    public void release()
    {
        release(intArrayAllocator);
    }

    public void release(IntArrayAllocator intArrayAllocator) {
        if (isMapOwned) {
            intArrayAllocator.store(rowNumberMap);
            isMapOwned = false;
            rowNumberMap = null;
        }
    }
}
