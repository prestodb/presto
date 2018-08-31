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
package com.facebook.presto.spi.type;

public class HashUtil
{
    private static final long XXHASH64_PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long XXHASH64_PRIME64_3 = 0x165667B19E3779F9L;

    private HashUtil() {}

    /**
     * <p>
     * This method is copied from {@code finalShuffle} from XxHash64 to provide non-linear transformation to
     * avoid hash collisions when computing checksum over structural types.
     *
     * <p>
     * When both structural hash and checksum are linear functions to input elements,
     * it's vulnerable to hash collision attack by exchanging some input values
     * (e.g. values in the same nested column).
     *
     * <p>
     * Example for checksum collision:
     *
     * <pre>
     * SELECT checksum(value)
     * FROM (VALUES
     *     ARRAY['a', '1'],
     *     ARRAY['b', '2']
     * ) AS t(value)
     * </pre>
     *
     * vs.
     *
     * <pre>
     * SELECT checksum(value)
     * FROM (VALUES
     *     ARRAY['a', '2'],
     *     ARRAY['b', '1']
     * ) AS t(value)
     * </pre>

     *
     * @see io.airlift.slice.XxHash64
     */
    public static long shuffle(long hash)
    {
        hash ^= hash >>> 33;
        hash *= XXHASH64_PRIME64_2;
        hash ^= hash >>> 29;
        hash *= XXHASH64_PRIME64_3;
        hash ^= hash >>> 32;
        return hash;
    }
}
