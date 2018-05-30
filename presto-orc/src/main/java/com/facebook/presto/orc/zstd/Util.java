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
package com.facebook.presto.orc.zstd;

import io.airlift.compress.MalformedInputException;

class Util
{
    private Util()
    {
    }

    public static int highestBit(int value)
    {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    public static boolean isPowerOf2(int value)
    {
        return (value & (value - 1)) == 0;
    }

    public static int mask(int bits)
    {
        return (1 << bits) - 1;
    }

    public static void verify(boolean condition, long offset, String reason)
    {
        if (!condition) {
            throw new MalformedInputException(offset, reason);
        }
    }

    public static MalformedInputException fail(long offset, String reason)
    {
        throw new MalformedInputException(offset, reason);
    }
}
