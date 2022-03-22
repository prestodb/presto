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

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.tdigest;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;

public final class TDigestUtils
{
    private TDigestUtils() {}

    static double weightedAverage(double x1, double w1, double x2, double w2)
    {
        if (x1 <= x2) {
            return weightedAverageSorted(x1, w1, x2, w2);
        }
        else {
            return weightedAverageSorted(x2, w2, x1, w1);
        }
    }

    private static double weightedAverageSorted(double x1, double w1, double x2, double w2)
    {
        final double x = (x1 * w1 + x2 * w2) / (w1 + w2);
        return Math.max(x1, Math.min(x, x2));
    }

    // Scale Functions
    public static double maxSize(double q, double compression, double n)
    {
        return Z(compression, n) * q * (1 - q) / compression;
    }

    public static double maxSize(double q, double normalizer)
    {
        return q * (1 - q) / normalizer;
    }

    public static double normalizer(double compression, double n)
    {
        return compression / Z(compression, n);
    }

    private static double Z(double compression, double n)
    {
        return 4 * Math.log(n / compression) + 24;
    }

    // Sorting Functions
    private static final Random prng = ThreadLocalRandom.current(); // for choosing pivots during quicksort

    /**
     * Quick sort using an index array.  On return,
     * values[order[i]] is in order as i goes 0..values.length
     *
     * @param order  Indexes into values
     * @param values The values to sort.
     */
    public static void sort(int[] order, double[] values)
    {
        sort(order, values, 0, values.length);
    }

    /**
     * Quick sort using an index array.  On return,
     * values[order[i]] is in order as i goes 0..n
     *
     * @param order  Indexes into values
     * @param values The values to sort.
     * @param n      The number of values to sort
     */
    public static void sort(int[] order, double[] values, int n)
    {
        sort(order, values, 0, n);
    }

    /**
     * Quick sort using an index array.  On return,
     * values[order[i]] is in order as i goes start..n
     *
     * @param order  Indexes into values
     * @param values The values to sort.
     * @param start  The first element to sort
     * @param n      The number of values to sort
     */
    public static void sort(int[] order, double[] values, int start, int n)
    {
        for (int i = start; i < start + n; i++) {
            order[i] = i;
        }
        quickSort(order, values, start, start + n, 64);
        insertionSort(order, values, start, start + n, 64);
    }

    /**
     * Standard quick sort except that sorting is done on an index array rather than the values themselves
     *
     * @param order  The pre-allocated index array
     * @param values The values to sort
     * @param start  The beginning of the values to sort
     * @param end    The value after the last value to sort
     * @param limit  The minimum size to recurse down to.
     */
    private static void quickSort(int[] order, double[] values, int start, int end, int limit)
    {
        // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
        while (end - start > limit) {
            // pivot by a random element
            int pivotIndex = start + prng.nextInt(end - start);
            double pivotValue = values[order[pivotIndex]];

            // move pivot to beginning of array
            swap(order, start, pivotIndex);

            // we use a three way partition because many duplicate values is an important case

            int low = start + 1;   // low points to first value not known to be equal to pivotValue
            int high = end;        // high points to first value > pivotValue
            int i = low;           // i scans the array
            while (i < high) {
                // invariant:  values[order[k]] == pivotValue for k in [0..low)
                // invariant:  values[order[k]] < pivotValue for k in [low..i)
                // invariant:  values[order[k]] > pivotValue for k in [high..end)
                // in-loop:  i < high
                // in-loop:  low < high
                // in-loop:  i >= low
                double vi = values[order[i]];
                if (vi == pivotValue) {
                    if (low != i) {
                        swap(order, low, i);
                    }
                    else {
                        i++;
                    }
                    low++;
                }
                else if (vi > pivotValue) {
                    high--;
                    swap(order, i, high);
                }
                else {
                    // vi < pivotValue
                    i++;
                }
            }
            // invariant:  values[order[k]] == pivotValue for k in [0..low)
            // invariant:  values[order[k]] < pivotValue for k in [low..i)
            // invariant:  values[order[k]] > pivotValue for k in [high..end)
            // assert i == high || low == high therefore, we are done with partition

            // at this point, i==high, from [start,low) are == pivot, [low,high) are < and [high,end) are >
            // we have to move the values equal to the pivot into the middle.  To do this, we swap pivot
            // values into the top end of the [low,high) range stopping when we run out of destinations
            // or when we run out of values to copy
            int from = start;
            int to = high - 1;
            for (i = 0; from < low && to >= low; i++) {
                swap(order, from++, to--);
            }
            if (from == low) {
                // ran out of things to copy.  This means that the the last destination is the boundary
                low = to + 1;
            }
            else {
                // ran out of places to copy to.  This means that there are uncopied pivots and the
                // boundary is at the beginning of those
                low = from;
            }

            // now recurse, but arrange it so we handle the longer limit by tail recursion
            if (low - start < end - high) {
                quickSort(order, values, start, low, limit);

                // this is really a way to do
                //    quickSort(order, values, high, end, limit);
                start = high;
            }
            else {
                quickSort(order, values, high, end, limit);
                // this is really a way to do
                //    quickSort(order, values, start, low, limit);
                end = low;
            }
        }
    }

    /**
     * Quick sort in place of several paired arrays.  On return,
     * keys[...] is in order and the values[] arrays will be
     * reordered as well in the same way.
     *
     * @param key    Values to sort on
     * @param values The auxilliary values to sort.
     */
    public static void sort(double[] key, double[]... values)
    {
        sort(key, 0, key.length, values);
    }

    /**
     * Quick sort using an index array.  On return,
     * values[order[i]] is in order as i goes start..n
     *  @param key    Values to sort on
     * @param start  The first element to sort
     * @param n      The number of values to sort
     * @param values The auxilliary values to sort.
     */
    public static void sort(double[] key, int start, int n, double[]... values)
    {
        quickSort(key, values, start, start + n, 8);
        insertionSort(key, values, start, start + n, 8);
    }

    /**
     * Standard quick sort except that sorting rearranges parallel arrays
     *
     * @param key    Values to sort on
     * @param values The auxilliary values to sort.
     * @param start  The beginning of the values to sort
     * @param end    The value after the last value to sort
     * @param limit  The minimum size to recurse down to.
     */
    private static void quickSort(double[] key, double[][] values, int start, int end, int limit)
    {
        // the while loop implements tail-recursion to avoid excessive stack calls on nasty cases
        while (end - start > limit) {
            // median of three values for the pivot
            int a = start;
            int b = (start + end) / 2;
            int c = end - 1;

            int pivotIndex;
            double pivotValue;
            double va = key[a];
            double vb = key[b];
            double vc = key[c];
            //noinspection Duplicates
            if (va > vb) {
                if (vc > va) {
                    // vc > va > vb
                    pivotIndex = a;
                    pivotValue = va;
                }
                else {
                    // va > vb, va >= vc
                    if (vc < vb) {
                        // va > vb > vc
                        pivotIndex = b;
                        pivotValue = vb;
                    }
                    else {
                        // va >= vc >= vb
                        pivotIndex = c;
                        pivotValue = vc;
                    }
                }
            }
            else {
                // vb >= va
                if (vc > vb) {
                    // vc > vb >= va
                    pivotIndex = b;
                    pivotValue = vb;
                }
                else {
                    // vb >= va, vb >= vc
                    if (vc < va) {
                        // vb >= va > vc
                        pivotIndex = a;
                        pivotValue = va;
                    }
                    else {
                        // vb >= vc >= va
                        pivotIndex = c;
                        pivotValue = vc;
                    }
                }
            }

            // move pivot to beginning of array
            swap(start, pivotIndex, key, values);

            // we use a three way partition because many duplicate values is an important case

            int low = start + 1;   // low points to first value not known to be equal to pivotValue
            int high = end;        // high points to first value > pivotValue
            int i = low;           // i scans the array
            while (i < high) {
                // invariant:  values[order[k]] == pivotValue for k in [0..low)
                // invariant:  values[order[k]] < pivotValue for k in [low..i)
                // invariant:  values[order[k]] > pivotValue for k in [high..end)
                // in-loop:  i < high
                // in-loop:  low < high
                // in-loop:  i >= low
                double vi = key[i];
                if (vi == pivotValue) {
                    if (low != i) {
                        swap(low, i, key, values);
                    }
                    else {
                        i++;
                    }
                    low++;
                }
                else if (vi > pivotValue) {
                    high--;
                    swap(i, high, key, values);
                }
                else {
                    // vi < pivotValue
                    i++;
                }
            }
            // invariant:  values[order[k]] == pivotValue for k in [0..low)
            // invariant:  values[order[k]] < pivotValue for k in [low..i)
            // invariant:  values[order[k]] > pivotValue for k in [high..end)
            // assert i == high || low == high therefore, we are done with partition

            // at this point, i==high, from [start,low) are == pivot, [low,high) are < and [high,end) are >
            // we have to move the values equal to the pivot into the middle.  To do this, we swap pivot
            // values into the top end of the [low,high) range stopping when we run out of destinations
            // or when we run out of values to copy
            int from = start;
            int to = high - 1;
            for (i = 0; from < low && to >= low; i++) {
                swap(from++, to--, key, values);
            }
            if (from == low) {
                // ran out of things to copy.  This means that the the last destination is the boundary
                low = to + 1;
            }
            else {
                // ran out of places to copy to.  This means that there are uncopied pivots and the
                // boundary is at the beginning of those
                low = from;
            }

            // now recurse, but arrange it so we handle the longer limit by tail recursion
            if (low - start < end - high) {
                quickSort(key, values, start, low, limit);

                // this is really a way to do
                //    quickSort(order, values, high, end, limit);
                start = high;
            }
            else {
                quickSort(key, values, high, end, limit);
                // this is really a way to do
                //    quickSort(order, values, start, low, limit);
                end = low;
            }
        }
    }

    /**
     * Limited range insertion sort.  We assume that no element has to move more than limit steps
     * because quick sort has done its thing. This version works on parallel arrays of keys and values.
     *
     * @param key    The array of keys
     * @param values The values we are sorting
     * @param start  The starting point of the sort
     * @param end    The ending point of the sort
     * @param limit  The largest amount of disorder
     */
    @SuppressWarnings("SameParameterValue")
    private static void insertionSort(double[] key, double[][] values, int start, int end, int limit)
    {
        // loop invariant: all values start ... i-1 are ordered
        for (int i = start + 1; i < end; i++) {
            double v = key[i];
            int m = Math.max(i - limit, start);
            for (int j = i; j >= m; j--) {
                if (j == m || key[j - 1] <= v) {
                    if (j < i) {
                        System.arraycopy(key, j, key, j + 1, i - j);
                        key[j] = v;
                        for (double[] value : values) {
                            double tmp = value[i];
                            System.arraycopy(value, j, value, j + 1, i - j);
                            value[j] = tmp;
                        }
                    }
                    break;
                }
            }
        }
    }

    private static void swap(int[] order, int i, int j)
    {
        int t = order[i];
        order[i] = order[j];
        order[j] = t;
    }

    private static void swap(int i, int j, double[] key, double[]...values)
    {
        double t = key[i];
        key[i] = key[j];
        key[j] = t;

        for (int k = 0; k < values.length; k++) {
            t = values[k][i];
            values[k][i] = values[k][j];
            values[k][j] = t;
        }
    }

    /**
     * Check that a partition step was done correctly.  For debugging and testing.
     *
     * @param order      The array of indexes representing a permutation of the keys.
     * @param values     The keys to sort.
     * @param pivotValue The value that splits the data
     * @param start      The beginning of the data of interest.
     * @param low        Values from start (inclusive) to low (exclusive) are &lt; pivotValue.
     * @param high       Values from low to high are equal to the pivot.
     * @param end        Values from high to end are above the pivot.
     */
    public static void checkPartition(int[] order, double[] values, double pivotValue, int start, int low, int high, int end)
    {
        if (order.length != values.length) {
            throw new IllegalArgumentException("Arguments must be same size");
        }

        if (!(start >= 0 && low >= start && high >= low && end >= high)) {
            throw new IllegalArgumentException(format("Invalid indices %d, %d, %d, %d", start, low, high, end));
        }

        for (int i = 0; i < low; i++) {
            double v = values[order[i]];
            if (v >= pivotValue) {
                throw new IllegalArgumentException(format("Value greater than pivot at %d", i));
            }
        }

        for (int i = low; i < high; i++) {
            if (values[order[i]] != pivotValue) {
                throw new IllegalArgumentException(format("Non-pivot at %d", i));
            }
        }

        for (int i = high; i < end; i++) {
            double v = values[order[i]];
            if (v <= pivotValue) {
                throw new IllegalArgumentException(format("Value less than pivot at %d", i));
            }
        }
    }

    /**
     * Limited range insertion sort.  We assume that no element has to move more than limit steps
     * because quick sort has done its thing.
     *
     * @param order  The permutation index
     * @param values The values we are sorting
     * @param start  Where to start the sort
     * @param n      How many elements to sort
     * @param limit  The largest amount of disorder
     */
    @SuppressWarnings("SameParameterValue")
    private static void insertionSort(int[] order, double[] values, int start, int n, int limit)
    {
        for (int i = start + 1; i < n; i++) {
            int t = order[i];
            double v = values[order[i]];
            int m = Math.max(i - limit, start);
            for (int j = i; j >= m; j--) {
                if (j == 0 || values[order[j - 1]] <= v) {
                    if (j < i) {
                        System.arraycopy(order, j, order, j + 1, i - j);
                        order[j] = t;
                    }
                    break;
                }
            }
        }
    }

    /**
     * Reverses an array in-place.
     *
     * @param order The array to reverse
     */
    public static void reverse(int[] order)
    {
        reverse(order, 0, order.length);
    }

    /**
     * Reverses part of an array. See {@link #reverse(int[])}
     *
     * @param order  The array containing the data to reverse.
     * @param offset Where to start reversing.
     * @param length How many elements to reverse
     */
    public static void reverse(int[] order, int offset, int length)
    {
        for (int i = 0; i < length / 2; i++) {
            int t = order[offset + i];
            order[offset + i] = order[offset + length - i - 1];
            order[offset + length - i - 1] = t;
        }
    }

    /**
     * Reverses part of an array. See {@link #reverse(int[])}
     *
     * @param order  The array containing the data to reverse.
     * @param offset Where to start reversing.
     * @param length How many elements to reverse
     */
    @SuppressWarnings("SameParameterValue")
    public static void reverse(double[] order, int offset, int length)
    {
        for (int i = 0; i < length / 2; i++) {
            double t = order[offset + i];
            order[offset + i] = order[offset + length - i - 1];
            order[offset + length - i - 1] = t;
        }
    }
}
