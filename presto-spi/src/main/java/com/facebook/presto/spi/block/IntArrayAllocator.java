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

import java.util.ArrayList;
import java.util.List;

public class IntArrayAllocator
{
    private List<int[]> arrays = new ArrayList();

    public int[] getIntArray(int size)
    {
        if (arrays.isEmpty()) {
            return new int[size];
        }
        int[] array = arrays.get(arrays.size() - 1);
        arrays.remove(arrays.size() - 1);
        if (array.length >= size) {
            return array;
        }
        return new int[size];
    }

    public void store(int[] array)
    {
        if (array.length < 1) {
            throw new IllegalArgumentException();
        }
        arrays.add(array);
    }
}
