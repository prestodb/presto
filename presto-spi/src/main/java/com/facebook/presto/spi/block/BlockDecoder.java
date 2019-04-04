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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BlockDecoder
{
    private static AtomicReference<int[]> identityMapReference = new AtomicReference<>(new int[] {0});
    private Block leafBlock;
    private Object values;
    private boolean[] valueIsNull;
    private int[] offsets;
    private int[] rowNumberMap;
    private int arrayOffset;
    private boolean isIdentityMap;
    // Ping pong buffers
    private int[] primaryIntCache;
    private int[] secondaryIntCache;

    public BlockDecoder()
    {
    }

    public void decodeBlock(Block block)
    {
        int positionCount = block.getPositionCount();
        isIdentityMap = true;
        values = null;
        offsets = null;
        rowNumberMap = null;
        int[] map = null;

        for (; ; ) {
            if (block instanceof DictionaryBlock) {
                isIdentityMap = false;
                int[] ids = ((DictionaryBlock) block).getIdsArray();
                int offset = ((DictionaryBlock) block).getIdsOffset();
                if (map == null) {
                    map = ids;
                    int[] newMap = getIntArray(positionCount);
                    System.arraycopy(map, offset, newMap, 0, positionCount);
                    map = newMap;
                }
                else {
                    if (isIdentityMap) {
                        int[] newMap = getIntArray(positionCount);
                        for (int i = 0; i < positionCount; ++i) {
                            newMap[i] = ids[map[i] + offset];
                        }
                        map = newMap;
                        isIdentityMap = false;
                    }
                    else {
                        for (int i = 0; i < positionCount; ++i) {
                            map[i] = ids[map[i] + offset];
                        }
                    }
                }
                block = ((DictionaryBlock) block).getDictionary();
            }
            else if (block instanceof RunLengthEncodedBlock) {
                if (map == null || isIdentityMap) {
                    map = getIntArray(positionCount);
                    isIdentityMap = false;
                }
                Arrays.fill(map, 0, positionCount, 0);
                block = ((RunLengthEncodedBlock) block).getValue();
            }
            else {
                leafBlock = block;
                block.getContents(this);
                if (arrayOffset != 0) {
                    if (!isIdentityMap) {
                        for (int i = 0; i < positionCount; i++) {
                            map[i] += arrayOffset;
                        }
                    }
                    else if (map == null) {
                        map = getIntArray(positionCount);
                        for (int i = 0; i < positionCount; i++) {
                            map[i] = i + arrayOffset;
                        }
                    }
                    else {
                        int[] newMap = getIntArray(positionCount);
                        System.arraycopy(map, 0, newMap, 0, positionCount);
                        for (int i = 0; i < positionCount; i++) {
                            newMap[i] += arrayOffset;
                        }
                        map = newMap;
                    }
                    isIdentityMap = false;
                    rowNumberMap = map;
                }
                else {
                    if (map == null) {
                        isIdentityMap = true;
                        rowNumberMap = getIdentityMap(positionCount);
                    }
                    else {
                        rowNumberMap = map;
                    }
                }
                return;
            }
        }
    }

    static int[] getIdentityMap(int size)
    {
        return identityMapReference.updateAndGet(map -> {
            if (map.length >= size) {
                return map;
            }
            int[] newMap = Arrays.copyOf(map, size);
            for (int i = map.length; i < newMap.length; i++) {
                newMap[i] = i;
            }
            return newMap;
        });
    }

    static int[] ensureCapacity(int[] map, int size)
    {
        if (map == null) {
            return new int[size];
        }
        if (map.length >= size) {
            return map;
        }
        return Arrays.copyOf(map, size);
    }

    public void release()
    {
        // TODO: what should the semantics of this be (if any).  Should it be close() to allow it
        // to be nested in try-with-resources?  Do we need to pass in a handle to something?
    }

    private int[] getIntArray(int positionCount)
    {
        primaryIntCache = ensureCapacity(primaryIntCache, positionCount);
        secondaryIntCache = ensureCapacity(secondaryIntCache, positionCount);
        int[] temp = primaryIntCache;
        primaryIntCache = secondaryIntCache;
        secondaryIntCache = temp;
        return temp;
    }

    public boolean[] getValueIsNull()
    {
        return valueIsNull;
    }

    public void setValueIsNull(boolean[] valueIsNull)
    {
        this.valueIsNull = valueIsNull;
    }

    @SuppressWarnings("unchecked cast")
    public <T> T getValues(Class<T> clazz)
    {
        requireNonNull(clazz, "clazz was null");
        if (!clazz.isAssignableFrom(values.getClass())) {
            throw new IllegalArgumentException(format(
                    "Error: decoded state %s incompatible with requested argument %s",
                    values.getClass(),
                    clazz));
        }
        return (T) values;
    }

    public void setValues(long[] values)
    {
        this.values = values;
    }

    public void setValues(double[] values)
    {
        this.values = values;
    }

    public void setValues(Slice values)
    {
        this.values = values;
    }

    public int[] getOffsets()
    {
        return offsets;
    }

    public void setOffsets(int[] offsets)
    {
        this.offsets = offsets;
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
        return isIdentityMap;
    }

    public int getArrayOffset()
    {
        return arrayOffset;
    }

    public void setArrayOffset(int arrayOffset)
    {
        this.arrayOffset = arrayOffset;
    }
}
