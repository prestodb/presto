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
package com.facebook.presto.common.block;

import java.util.Arrays;

import static com.facebook.presto.common.block.BlockUtil.MAX_ARRAY_SIZE;
import static java.lang.String.format;

/**
 * A simplified version of fastutils IntArrayList for the purpose of positions copying.
 */
class IntArrayList
{
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private int[] array;
    private int size;

    IntArrayList(int initialCapacity)
    {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException(format("Initial capacity '%s' is negative", initialCapacity));
        }
        array = new int[initialCapacity];
    }

    IntArrayList()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    int[] elements()
    {
        return array;
    }

    private void grow(int newCapacity)
    {
        if (array.length == MAX_ARRAY_SIZE) {
            throw new IllegalStateException("Array reached maximum size");
        }

        if (newCapacity > array.length) {
            int newLength = (int) Math.min(Math.max(2L * (long) array.length, (long) newCapacity), MAX_ARRAY_SIZE);
            array = Arrays.copyOf(array, newLength);
        }
    }

    void add(int element)
    {
        grow(size + 1);
        array[size++] = element;
    }

    int size()
    {
        return size;
    }

    boolean isEmpty()
    {
        return size == 0;
    }
}
