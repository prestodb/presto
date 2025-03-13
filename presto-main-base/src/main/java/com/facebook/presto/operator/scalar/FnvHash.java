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
package com.facebook.presto.operator.scalar;

import io.airlift.slice.Slice;

/**
 * Reference implementation: https://tools.ietf.org/html/draft-eastlake-fnv-17#section-6
 * TODO: Move this utility class to airlift:slice
 */
public class FnvHash
{
    private static final long FNV_64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_64_PRIME = 0x100000001b3L;

    private static final int FNV_32_OFFSET_BASIS = 0x811c9dc5;
    private static final int FNV_32_PRIME = 0x01000193;

    private FnvHash()
    {
    }

    public static int fnv1Hash32(Slice data)
    {
        int hash = FNV_32_OFFSET_BASIS;

        for (int i = 0; i < data.length(); ++i) {
            int dataByte = data.getUnsignedByte(i);
            hash *= FNV_32_PRIME;
            hash ^= dataByte;
        }

        return hash;
    }

    public static long fnv1Hash64(Slice data)
    {
        long hash = FNV_64_OFFSET_BASIS;

        for (int i = 0; i < data.length(); ++i) {
            long dataByte = data.getUnsignedByte(i);
            hash *= FNV_64_PRIME;
            hash ^= dataByte;
        }

        return hash;
    }

    public static int fnv1aHash32(Slice data)
    {
        int hash = FNV_32_OFFSET_BASIS;

        for (int i = 0; i < data.length(); ++i) {
            int dataByte = data.getUnsignedByte(i);
            hash ^= dataByte;
            hash *= FNV_32_PRIME;
        }

        return hash;
    }

    public static long fnv1aHash64(Slice data)
    {
        long hash = FNV_64_OFFSET_BASIS;

        for (int i = 0; i < data.length(); ++i) {
            long dataByte = data.getUnsignedByte(i);
            hash ^= dataByte;
            hash *= FNV_64_PRIME;
        }

        return hash;
    }
}
