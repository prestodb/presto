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
package com.facebook.presto.common.array;

import java.io.Serializable;

import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * This class is adapted from it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap.
 * Same as the original Long2IntOpenHashMap, it treats key 0 as null.
 * The initial value for any key is defined as defRetValue, and can be set through setDefaultReturnValue(). By default it is 0.
 * Not thread safe.
 */
public class Long2IntOpenHashMap
        implements Serializable, Cloneable
{
    protected static final float FILL_FACTOR = 0.75f;
    protected static final int INITIAL_ELEMENT_COUNT = 16;

    protected transient long[] key;
    protected transient int[] value;
    protected transient int mask;
    protected transient boolean containsNullKey;
    protected transient int n;
    protected transient int maxFill;
    protected final transient int minN;
    protected int size;
    protected int defRetValue;

    public Long2IntOpenHashMap()
    {
        n = arraySize(INITIAL_ELEMENT_COUNT, FILL_FACTOR);
        minN = n;
        mask = n - 1;
        maxFill = maxFill(n, FILL_FACTOR);
        key = new long[n + 1];
        value = new int[n + 1];
    }

    public void setDefaultReturnValue(int rv)
    {
        defRetValue = rv;
    }

    public int getDefaultReturnValue()
    {
        return defRetValue;
    }

    /**
     * Add incr to the existing value for key k.
     *
     * @param k key
     * @param incr The increment value to be added to the existing value for key k
     * @return The updated value for key k
     */
    public int addTo(long k, int incr)
    {
        int pos;
        if (k == 0L) {
            if (containsNullKey) {
                return addToValue(n, incr);
            }

            pos = n;
            containsNullKey = true;
        }
        else {
            long[] key = this.key;

            pos = (int) mix(k) & mask;
            long curr = key[pos];

            if (curr != 0L) {
                if (curr == k) {
                    return addToValue(pos, incr);
                }

                while (curr != 0L) {
                    if (curr == k) {
                        return addToValue(pos, incr);
                    }
                    pos = pos + 1 & mask;
                    curr = key[pos];
                }
            }
        }

        key[pos] = k;
        value[pos] = defRetValue + incr;
        if (size++ >= maxFill) {
            rehash(arraySize(size + 1, FILL_FACTOR));
        }

        return defRetValue;
    }

    /**
     * Get the value for key k
     *
     * @param k key
     * @return value for key k
     */
    public int get(long k)
    {
        if (k == 0L) {
            return containsNullKey ? value[n] : defRetValue;
        }
        else {
            long[] key = this.key;

            int pos = (int) mix(k) & mask;
            long curr = key[pos];
            if (curr == 0L) {
                return defRetValue;
            }
            else if (k == curr) {
                return value[pos];
            }
            else {
                while (curr != 0L) {
                    if (k == curr) {
                        return value[pos];
                    }
                    pos = pos + 1 & mask;
                    curr = key[pos];
                }

                return defRetValue;
            }
        }
    }

    /**
     * Remove key k and its corresponding value from the map
     *
     * @param k key
     * @return The old value for key k before it's removed
     */
    public int remove(long k)
    {
        if (k == 0L) {
            return containsNullKey ? removeNullEntry() : defRetValue;
        }
        else {
            long[] key = this.key;

            int pos = (int) mix(k) & mask;
            long curr = key[pos];
            if (curr == 0L) {
                return defRetValue;
            }
            else if (k == curr) {
                return removeEntry(pos);
            }
            else {
                while (curr != 0L) {
                    if (k == curr) {
                        return removeEntry(pos);
                    }
                    pos = pos + 1 & mask;
                    curr = key[pos];
                }

                return defRetValue;
            }
        }
    }

    /**
     * Return the number of entries in the map
     *
     * @return the number of entries in the map
     */
    public int size()
    {
        return size;
    }

    protected void rehash(int newN)
    {
        long[] key = this.key;
        int[] value = this.value;
        int mask = newN - 1;
        long[] newKey = new long[newN + 1];
        int[] newValue = new int[newN + 1];
        int i = this.n;

        int pos;
        for (int size = realSize(); size-- != 0; newValue[pos] = value[i]) {
            do {
                --i;
            }
            while (key[i] == 0L);

            pos = (int) mix(key[i]) & mask;
            if (newKey[pos] != 0L) {
                while (newKey[pos] != 0L) {
                    pos = pos + 1 & mask;
                }
            }

            newKey[pos] = key[i];
        }

        newValue[newN] = value[n];
        this.n = newN;
        this.mask = mask;
        this.maxFill = maxFill(n, FILL_FACTOR);
        this.key = newKey;
        this.value = newValue;
    }

    private int addToValue(int pos, int incr)
    {
        int oldValue = value[pos];
        value[pos] = oldValue + incr;
        return oldValue;
    }

    private int removeNullEntry()
    {
        containsNullKey = false;
        int oldValue = value[n];
        --size;
        if (n > minN && size < maxFill / 4 && n > 16) {
            rehash(n / 2);
        }

        return oldValue;
    }

    private int removeEntry(int pos)
    {
        int oldValue = value[pos];
        --size;
        shiftKeys(pos);
        if (n > minN && size < maxFill / 4 && n > 16) {
            rehash(n / 2);
        }

        return oldValue;
    }

    private static int arraySize(int expected, float f)
    {
        long s = max(2L, nextPowerOfTwo((long) ceil((double) ((float) expected / f))));
        if (s > 1073741824L) {
            throw new IllegalArgumentException("Too large (" + expected + " expected elements with load factor " + f + ")");
        }
        else {
            return (int) s;
        }
    }

    private static int maxFill(int n, float f)
    {
        return min((int) ceil((double) ((float) n * f)), n - 1);
    }

    public static long mix(long x)
    {
        long h = x * -7046029254386353131L;
        h ^= h >>> 32;
        return h ^ h >>> 16;
    }

    protected final void shiftKeys(int pos)
    {
        long[] key = this.key;

        while (true) {
            int last = pos;
            pos = pos + 1 & mask;

            long curr;
            while (true) {
                curr = key[pos];
                if (curr == 0L) {
                    key[last] = 0L;
                    return;
                }

                int slot = (int) mix(curr) & mask;
                if (last <= pos) {
                    if (last >= slot || slot > pos) {
                        break;
                    }
                }
                else if (last >= slot && slot > pos) {
                    break;
                }

                pos = pos + 1 & mask;
            }

            key[last] = curr;
            value[last] = value[pos];
        }
    }

    private int realSize()
    {
        return containsNullKey ? size - 1 : size;
    }

    private static long nextPowerOfTwo(long x)
    {
        if (x == 0L) {
            return 1L;
        }
        else {
            --x;
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            return (x | x >> 32) + 1L;
        }
    }
}
