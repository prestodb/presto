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
package com.facebook.presto.array;

public class Arrays
{
    private Arrays() {}

    public static int[] ensureCapacity(int[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new int[capacity];
        }

        return buffer;
    }

    public static long[] ensureCapacity(long[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new long[capacity];
        }

        return buffer;
    }

    public static boolean[] ensureCapacity(boolean[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[capacity];
        }

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new byte[capacity];
        }

        return buffer;
    }
}
