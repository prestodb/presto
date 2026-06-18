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
package com.facebook.presto.geospatial.rtree;

import com.facebook.presto.geospatial.Rectangle;

import static java.util.Objects.requireNonNull;

/**
 * A class that can calculate Hilbert indices.
 * <p>
 * A Hilbert index is the index on a Hilbert curve; a Hilbert curve is a
 * space-filling curve in a rectangle.  This class is instantiated with the
 * rectangle within which it can calculate the index.
 * <p>
 * The (fast) index algorithm is adapted from the C++ from
 * https://github.com/rawrunprotected/hilbert_curves ,
 * original algorithm by http://threadlocalmutex.com/?p=126 .
 */
public class HilbertIndex
{
    private static final int HILBERT_BITS = 16;
    private static final double HILBERT_MAX = (1 << HILBERT_BITS) - 1;

    private final Rectangle rectangle;
    private final double xScale;
    private final double yScale;

    /**
     * @param rectangle Rectangle defining bounds of Hilbert curve
     */
    public HilbertIndex(Rectangle rectangle)
    {
        this.rectangle = requireNonNull(rectangle, "rectangle is null");
        if (rectangle.getXMax() == rectangle.getXMin()) {
            this.xScale = 0;
        }
        else {
            this.xScale = HILBERT_MAX / (rectangle.getXMax() - rectangle.getXMin());
        }
        if (rectangle.getYMax() == rectangle.getYMin()) {
            this.yScale = 0;
        }
        else {
            this.yScale = HILBERT_MAX / (rectangle.getYMax() - rectangle.getYMin());
        }
    }

    /**
     * Calculate Hilbert index of coordinates in rectangle.
     * <p>
     * This gives a reasonable index for coordinates contained in the bounding
     * rectangle; coordinates not in the box will return `Long.MAX_VALUE`.
     *
     * @param x
     * @param y
     * @return Hilbert curve index, relative to rectangle
     */
    public long indexOf(double x, double y)
    {
        if (!rectangle.contains(x, y)) {
            // Put things outside the box at the end
            // This will also handle infinities and NaNs
            return Long.MAX_VALUE;
        }

        int xInt = (int) (xScale * (x - rectangle.getXMin()));
        int yInt = (int) (yScale * (y - rectangle.getYMin()));
        return discreteIndexOf(xInt, yInt);
    }

    /**
     * Calculate the Hilbert index of a discrete coordinate.
     * <p>
     * Since Java doesn't have unsigned ints, we put incoming ints into the
     * lower 32 bits of a long and do the calculations there.
     *
     * @param x discrete positive x coordinate
     * @param y discrete positive y coordinate
     * @return Hilbert curve index
     */
    private long discreteIndexOf(int x, int y)
    {
        int a = x ^ y;
        int b = 0x0000FFFF ^ a;
        int c = 0x0000FFFF ^ (x | y);
        int d = x & (y ^ 0x0000FFFF);

        int e = a | (b >>> 1);
        int f = (a >>> 1) ^ a;
        int g = ((c >>> 1) ^ (b & (d >>> 1))) ^ c;
        int h = ((a & (c >>> 1)) ^ (d >>> 1)) ^ d;

        a = e;
        b = f;
        c = g;
        d = h;
        e = (a & (a >>> 2)) ^ (b & (b >>> 2));
        f = (a & (b >>> 2)) ^ (b & ((a ^ b) >>> 2));
        g ^= (a & (c >>> 2)) ^ (b & (d >>> 2));
        h ^= (b & (c >>> 2)) ^ ((a ^ b) & (d >>> 2));

        a = e;
        b = f;
        c = g;
        d = h;
        e = (a & (a >>> 4)) ^ (b & (b >>> 4));
        f = (a & (b >>> 4)) ^ (b & ((a ^ b) >>> 4));
        g ^= (a & (c >>> 4)) ^ (b & (d >>> 4));
        h ^= (b & (c >>> 4)) ^ ((a ^ b) & (d >>> 4));

        a = e;
        b = f;
        c = g;
        d = h;
        g ^= (a & (c >>> 8)) ^ (b & (d >>> 8));
        h ^= (b & (c >>> 8)) ^ ((a ^ b) & (d >>> 8));

        a = (g ^ (g >>> 1));
        b = (h ^ (h >>> 1));

        int i0 = (x ^ y);
        int i1 = (b | (0x0000FFFF ^ (i0 | a)));

        i0 = (i0 | (i0 << 8)) & 0x00FF00FF;
        i0 = (i0 | (i0 << 4)) & 0x0F0F0F0F;
        i0 = (i0 | (i0 << 2)) & 0x33333333;
        i0 = (i0 | (i0 << 1)) & 0x55555555;

        i1 = (i1 | (i1 << 8)) & 0x00FF00FF;
        i1 = (i1 | (i1 << 4)) & 0x0F0F0F0F;
        i1 = (i1 | (i1 << 2)) & 0x33333333;
        i1 = (i1 | (i1 << 1)) & 0x55555555;

        return (((long) ((i1 << 1) | i0)) << 32) >>> 32;
    }
}
