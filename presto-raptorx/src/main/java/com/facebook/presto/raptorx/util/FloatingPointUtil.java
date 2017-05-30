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
package com.facebook.presto.raptorx.util;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

public final class FloatingPointUtil
{
    private FloatingPointUtil() {}

    /**
     * Converts a double value to a sortable long. The value is converted by swapping
     * some bits in the IEEE 754 layout to be able to compare the result as a long.
     *
     * @see #sortableLongToDouble(long)
     */
    public static long doubleToSortableLong(double value)
    {
        long bits = doubleToLongBits(value);
        return bits ^ ((bits >> 63) & Long.MAX_VALUE);
    }

    /**
     * Converts a sortable long to double.
     *
     * @see #doubleToSortableLong(double)
     */
    public static double sortableLongToDouble(long value)
    {
        value ^= (value >> 63) & Long.MAX_VALUE;
        return longBitsToDouble(value);
    }

    /**
     * Converts a float value to a sortable int. The value is converted by swapping
     * some bits in the IEEE 754 layout to be able to compare the result as a int.
     *
     * @see #sortableIntToFloat(int)
     */
    public static int floatToSortableInt(float value)
    {
        int bits = floatToIntBits(value);
        return bits ^ ((bits >> 31) & Integer.MAX_VALUE);
    }

    /**
     * Converts a sortable int to float.
     *
     * @see #floatToSortableInt(float)
     */
    public static float sortableIntToFloat(int value)
    {
        value ^= (value >> 31) & Integer.MAX_VALUE;
        return intBitsToFloat(value);
    }
}
