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
package com.facebook.presto.operator;

import static java.util.Objects.requireNonNull;

public class PagesIndexOrdering
{
    private static final int SMALL = 7;
    private static final int MEDIUM = 40;

    private final PagesIndexComparator comparator;

    public PagesIndexOrdering(PagesIndexComparator comparator)
    {
        this.comparator = requireNonNull(comparator, "comparator is null");
    }

    public PagesIndexComparator getComparator()
    {
        return comparator;
    }

    public void sort(PagesIndex pagesIndex, int startPosition, int endPosition)
    {
        quickSort(pagesIndex, startPosition, endPosition);
    }

    /**
     * Sorts the specified range of elements using the specified swapper and according to the order induced by the specified
     * comparator using quickSort.
     * <p/>
     * <p>The sorting algorithm is a tuned quickSort adapted from Jon L. Bentley and M. Douglas
     * McIlroy, &ldquo;Engineering a Sort Function&rdquo;, <i>Software: Practice and Experience</i>, 23(11), pages
     * 1249&minus;1265, 1993.
     *
     * @param from the index of the first element (inclusive) to be sorted.
     * @param to the index of the last element (exclusive) to be sorted.
     */
    // note this code was forked from Fastutils
    @SuppressWarnings("InnerAssignment")
    private void quickSort(PagesIndex pagesIndex, int from, int to)
    {
        int len = to - from;
        // Insertion sort on smallest arrays
        if (len < SMALL) {
            for (int i = from; i < to; i++) {
                for (int j = i; j > from && (comparator.compareTo(pagesIndex, j - 1, j) > 0); j--) {
                    pagesIndex.swap(j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = from + len / 2; // Small arrays, middle element
        if (len > SMALL) {
            int l = from;
            int n = to - 1;
            if (len > MEDIUM) { // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = median3(pagesIndex, l, l + s, l + 2 * s);
                m = median3(pagesIndex, m - s, m, m + s);
                n = median3(pagesIndex, n - 2 * s, n - s, n);
            }
            m = median3(pagesIndex, l, m, n); // Mid-size, med of 3
        }
        // int v = x[m];

        int a = from;
        int b = a;
        int c = to - 1;
        // Establish Invariant: v* (<v)* (>v)* v*
        int d = c;
        while (true) {
            int comparison;
            while (b <= c && ((comparison = comparator.compareTo(pagesIndex, b, m)) <= 0)) {
                if (comparison == 0) {
                    if (a == m) {
                        m = b; // moving target; DELTA to JDK !!!
                    }
                    else if (b == m) {
                        m = a; // moving target; DELTA to JDK !!!
                    }
                    pagesIndex.swap(a++, b);
                }
                b++;
            }
            while (c >= b && ((comparison = comparator.compareTo(pagesIndex, c, m)) >= 0)) {
                if (comparison == 0) {
                    if (c == m) {
                        m = d; // moving target; DELTA to JDK !!!
                    }
                    else if (d == m) {
                        m = c; // moving target; DELTA to JDK !!!
                    }
                    pagesIndex.swap(c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            if (b == m) {
                m = d; // moving target; DELTA to JDK !!!
            }
            else if (c == m) {
                m = c; // moving target; DELTA to JDK !!!
            }
            pagesIndex.swap(b++, c--);
        }

        // Swap partition elements back to middle
        int s;
        int n = to;
        s = Math.min(a - from, b - a);
        vectorSwap(pagesIndex, from, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vectorSwap(pagesIndex, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            quickSort(pagesIndex, from, from + s);
        }
        if ((s = d - c) > 1) {
            quickSort(pagesIndex, n - s, n);
        }
    }

    /**
     * Returns the index of the median of the three positions.
     */
    private int median3(PagesIndex pagesIndex, int a, int b, int c)
    {
        int ab = comparator.compareTo(pagesIndex, a, b);
        int ac = comparator.compareTo(pagesIndex, a, c);
        int bc = comparator.compareTo(pagesIndex, b, c);
        return (ab < 0 ?
                (bc < 0 ? b : ac < 0 ? c : a) :
                (bc > 0 ? b : ac > 0 ? c : a));
    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vectorSwap(PagesIndex pagesIndex, int from, int l, int s)
    {
        for (int i = 0; i < s; i++, from++, l++) {
            pagesIndex.swap(from, l);
        }
    }
}
