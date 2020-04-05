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
package com.facebook.presto.spi.memory;

import java.util.Arrays;

class LongArrayList
{
    long[] longs = new long[10];
    int size = 0;

    void add(long value)
    {
        if (longs.length <= size) {
            longs = Arrays.copyOf(longs, 2 * size);
        }
        longs[size++] = value;
    }


    int size()
    {
        return size;
    }

    long get(int i)
    {
        return longs[i];
    }

    void popBack()
    {
        size--;
    }
}