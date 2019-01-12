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
package io.prestosql.array;

import static io.prestosql.array.BigArrays.SEGMENT_SHIFT;
import static io.prestosql.array.BigArrays.SEGMENT_SIZE;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public class IntBigArrays
{
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;
    private static final int SMALL = 7;
    private static final int MEDIUM = 40;

    private IntBigArrays()
    {}

    /**
     * Computes the segment associated with a given index.
     *
     * @param index an index into a big array.
     * @return the associated segment.
     */
    public static int segment(final long index)
    {
        return (int) (index >>> SEGMENT_SHIFT);
    }

    /**
     * Computes the displacement associated with a given index.
     *
     * @param index an index into a big array.
     * @return the associated displacement (in the associated {@linkplain #segment(long) segment}).
     */
    public static int displacement(final long index)
    {
        return (int) (index & SEGMENT_MASK);
    }

    /**
     * Returns the element of the given big array of specified index.
     *
     * @param array a big array.
     * @param index a position in the big array.
     * @return the element of the big array at the specified position.
     */
    public static int get(final int[][] array, final long index)
    {
        return array[segment(index)][displacement(index)];
    }

    /**
     * Sets the element of the given big array of specified index.
     *
     * @param array a big array.
     * @param index a position in the big array.
     */
    public static void set(final int[][] array, final long index, int value)
    {
        array[segment(index)][displacement(index)] = value;
    }

    /**
     * Swaps the element of the given big array of specified indices.
     *
     * @param array a big array.
     * @param first a position in the big array.
     * @param second a position in the big array.
     */
    public static void swap(final int[][] array, final long first, final long second)
    {
        final int t = array[segment(first)][displacement(first)];
        array[segment(first)][displacement(first)] = array[segment(second)][displacement(second)];
        array[segment(second)][displacement(second)] = t;
    }

    /**
     * Sorts the specified range of elements according to the order induced by the specified
     * comparator using quicksort.
     * <p>
     * <p>The sorting algorithm is a tuned quicksort adapted from Jon L. Bentley and M. Douglas
     * McIlroy, &ldquo;Engineering a Sort Function&rdquo;, <i>Software: Practice and Experience</i>, 23(11), pages
     * 1249&minus;1265, 1993.
     *
     * @param x the big array to be sorted.
     * @param from the index of the first element (inclusive) to be sorted.
     * @param to the index of the last element (exclusive) to be sorted.
     * @param comp the comparator to determine the sorting order.
     */
    @SuppressWarnings("checkstyle:InnerAssignment")
    public static void quickSort(final int[][] x, final long from, final long to, final IntComparator comp)
    {
        final long len = to - from;
        // Selection sort on smallest arrays
        if (len < SMALL) {
            selectionSort(x, from, to, comp);
            return;
        }
        // Choose a partition element, v
        long m = from + len / 2; // Small arrays, middle element
        if (len > SMALL) {
            long l = from;
            long n = to - 1;
            if (len > MEDIUM) { // Big arrays, pseudomedian of 9
                long s = len / 8;
                l = med3(x, l, l + s, l + 2 * s, comp);
                m = med3(x, m - s, m, m + s, comp);
                n = med3(x, n - 2 * s, n - s, n, comp);
            }
            m = med3(x, l, m, n, comp); // Mid-size, med of 3
        }
        final int v = get(x, m);
        // Establish Invariant: v* (<v)* (>v)* v*
        long a = from;
        long b = a;
        long c = to - 1;
        long d = c;
        while (true) {
            int comparison;
            while (b <= c && (comparison = comp.compare(get(x, b), v)) <= 0) {
                if (comparison == 0) {
                    swap(x, a++, b);
                }
                b++;
            }
            while (c >= b && (comparison = comp.compare(get(x, c), v)) >= 0) {
                if (comparison == 0) {
                    swap(x, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swap(x, b++, c--);
        }
        // Swap partition elements back to middle
        long s;
        long n = to;
        s = Math.min(a - from, b - a);
        vecSwap(x, from, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecSwap(x, b, n - s, s);
        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            quickSort(x, from, from + s, comp);
        }
        if ((s = d - c) > 1) {
            quickSort(x, n - s, n, comp);
        }
    }

    private static void vecSwap(final int[][] x, long a, long b, final long n)
    {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(x, a, b);
        }
    }

    private static long med3(final int[][] x, final long a, final long b, final long c, IntComparator comp)
    {
        int ab = comp.compare(get(x, a), get(x, b));
        int ac = comp.compare(get(x, a), get(x, c));
        int bc = comp.compare(get(x, b), get(x, c));
        return (ab < 0 ?
                (bc < 0 ? b : ac < 0 ? c : a) :
                (bc > 0 ? b : ac > 0 ? c : a));
    }

    private static void selectionSort(final int[][] a, final long from, final long to, final IntComparator comp)
    {
        for (long i = from; i < to - 1; i++) {
            long m = i;
            for (long j = i + 1; j < to; j++) {
                if (comp.compare(IntBigArrays.get(a, j), IntBigArrays.get(a, m)) < 0) {
                    m = j;
                }
            }
            if (m != i) {
                swap(a, i, m);
            }
        }
    }
}
