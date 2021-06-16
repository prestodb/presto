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
package com.facebook.presto.common.zorder;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestZOrder
{
    @Test
    public void testZOrder()
            throws Exception
    {
        List<Integer> intColumns = new ArrayList<>();
        intColumns.add(54813);
        intColumns.add(97);
        byte[] address = ZOrder.encodeIntegers(intColumns);
        List<Integer> decodedIntCols = ZOrder.decodeIntegers(2, address);
        if (!intColumns.toString().equals(decodedIntCols.toString())) {
            throw new Exception("Integers decoded improperly");
        }

        intColumns.set(0, 214);
        address = ZOrder.encodeIntegers(intColumns);
        decodedIntCols = ZOrder.decodeIntegers(2, address);
        if (!intColumns.toString().equals(decodedIntCols.toString())) {
            throw new Exception("Integers decoded improperly");
        }

        intColumns.set(0, 16);
        intColumns.set(1, 128);
        intColumns.add(419);
        address = ZOrder.encodeIntegers(intColumns);
        decodedIntCols = ZOrder.decodeIntegers(3, address);
        if (!intColumns.toString().equals(decodedIntCols.toString())) {
            throw new Exception("Integers decoded improperly");
        }

        List<Float> floatCols = new ArrayList<>();
        floatCols.add(1.00F);
        floatCols.add(0.356F);
        address = ZOrder.encodeFloats(floatCols);
        List<Float> decodedFloatCols = ZOrder.decodeFloats(2, address);
        System.out.println(floatCols.toString());
        System.out.println(decodedFloatCols.toString());
    }

    @Test
    public void debugZOrder()
            throws Exception
    {
        List<Integer> intColumns = new ArrayList<>(); // 56, 102, 100, 114, 27, 9, 124
        intColumns.add(56);
        intColumns.add(102);
        intColumns.add(100);
        intColumns.add(114);
        intColumns.add(27);
        intColumns.add(9);
        intColumns.add(124);
        List<Integer> ranges = new ArrayList<>();
        ranges.add(7);
        ranges.add(8);
        ranges.add(8);
        ranges.add(8);
        ranges.add(6);
        ranges.add(5);
        ranges.add(8);
        byte[] address1 = ZOrder.encodeIntegers(intColumns, ranges);
        System.out.println(intColumns.toString());
        ZOrder.printBits(address1);
        List<Integer> decodedCols = ZOrder.decodeIntegers(ranges, address1);
        System.out.println(decodedCols.toString());
        // long address2 = ZOrder.encodeIntegers(intColumns);
        // System.out.println(Long.toBinaryString(address2) + ": " + address2);
    }

    @Test
    public void testZOrderTime()
            throws Exception
    {
        int runs = 10;
        int totalTime = 0;
        int cols = 7;
        int loops = 10000000;

        List<Integer> intColumns = new ArrayList<>(cols);
        intColumns.add(13);
        intColumns.add(97);
        intColumns.add(116);
        intColumns.add(127);
        intColumns.add(24);
        intColumns.add(108);
        intColumns.add(76);
        byte[] address = ZOrder.encodeIntegers(intColumns);
        List<Integer> decodedIntCols = ZOrder.decodeIntegers(cols, address);
        // System.out.println(Long.toBinaryString(address) + ": " + address);
        if (!intColumns.toString().equals(decodedIntCols.toString())) {
            throw new Exception("Integers decoded improperly");
        }
        Random rand = new Random();
        int[][] intCols = new int[512][cols];
        for (int i = 0; i < 512; i++) {
            for (int c = 0; c < cols; c++) {
                intCols[i][c] = rand.nextInt(128);
            }
        }

        for (int x = 0; x < runs; x++) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < loops; i++) {
                for (int idx = 0; idx < cols; idx++) {
                    intColumns.set(idx, intCols[i & 511][idx]);
                }
                address = ZOrder.encodeIntegers(intColumns);
                decodedIntCols = ZOrder.decodeIntegers(cols, address);
                // System.out.println(Long.toBinaryString(address) + ": " + address);
                if (!intColumns.toString().equals(decodedIntCols.toString())) {
                    System.out.println(intColumns.toString());
                    System.out.println(decodedIntCols.toString());
                    throw new Exception("Integers decoded improperly");
                }
            }
            time = System.currentTimeMillis() - time;
            totalTime += time;
            System.out.println(time);
        }
        System.out.println("Average time of " + runs + " runs: " + totalTime / runs);
    }

    private static int getHighestSetBitPosition(long value)
    {
        // Assumes value is non-negative
        int position = 0;
        while (value != 0) {
            value >>= 1;
            position++;
        }
        return position;
    }

    @Test
    public void testZOrderRangeTime()
            throws Exception
    {
        int runs = 10;
        int totalTime = 0;
        int cols = 7;
        int loops = 10000000;

        List<Integer> intColumns = new ArrayList<>(cols);
        intColumns.add(13);
        intColumns.add(97);
        intColumns.add(116);
        intColumns.add(127);
        intColumns.add(24);
        intColumns.add(108);
        intColumns.add(76);
        List<Integer> intRanges = new ArrayList<>(cols);
        intRanges.add(8);
        intRanges.add(8);
        intRanges.add(16);
        intRanges.add(32);
        intRanges.add(8);
        intRanges.add(8);
        intRanges.add(8);
        byte[] address = ZOrder.encodeIntegers(intColumns, intRanges);
        List<Integer> decodedIntCols = ZOrder.decodeIntegers(intRanges, address);
        // System.out.println(Long.toBinaryString(address) + ": " + address);
        if (!intColumns.toString().equals(decodedIntCols.toString())) {
            throw new Exception("Integers decoded improperly");
        }
        Random rand = new Random();
        int[][] intCols = new int[512][cols];
        int[][] ranges = new int[512][cols];
        for (int i = 0; i < 512; i++) {
            for (int c = 0; c < cols; c++) {
                intCols[i][c] = rand.nextInt(128);
                ranges[i][c] = getHighestSetBitPosition(intCols[i][c]) + 1;
            }
        }
        for (int x = 0; x < runs; x++) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < loops; i++) {
                for (int idx = 0; idx < cols; idx++) {
                    intColumns.set(idx, intCols[i & 511][idx]);
                    intRanges.set(idx, ranges[i & 511][idx]);
                }
                address = ZOrder.encodeIntegers(intColumns, intRanges);
                decodedIntCols = ZOrder.decodeIntegers(intRanges, address);
                // System.out.println(Long.toBinaryString(address) + ": " + address);
                if (!intColumns.toString().equals(decodedIntCols.toString())) {
                    System.out.println(intColumns.toString());
                    System.out.println(decodedIntCols.toString());
                    System.out.println(intRanges.toString());
                    throw new Exception("Integers decoded improperly");
                }
            }
            time = System.currentTimeMillis() - time;
            totalTime += time;
            System.out.println(time);
        }
        System.out.println("Average time of " + runs + " runs: " + totalTime / runs);
    }

    @Test
    public void testZOrderEncodeRangeTime()
            throws Exception
    {
        int runs = 10;
        int totalTime = 0;
        int cols = 7;
        int loops = 10000000;

        List<Integer> intColumns = new ArrayList<>(cols);
        intColumns.add(13);
        intColumns.add(97);
        intColumns.add(116);
        intColumns.add(127);
        intColumns.add(24);
        intColumns.add(108);
        intColumns.add(76);
        List<Integer> intRanges = new ArrayList<>(cols);
        intRanges.add(8);
        intRanges.add(8);
        intRanges.add(16);
        intRanges.add(32);
        intRanges.add(8);
        intRanges.add(8);
        intRanges.add(8);
        ZOrder.encodeIntegers(intColumns, intRanges);
        Random rand = new Random();
        int[][] intCols = new int[512][cols];
        int[][] ranges = new int[512][cols];
        for (int i = 0; i < 512; i++) {
            for (int c = 0; c < cols; c++) {
                intCols[i][c] = rand.nextInt(128);
                ranges[i][c] = getHighestSetBitPosition(intCols[i][c]) + 1;
            }
        }
        for (int x = 0; x < runs; x++) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < loops; i++) {
                for (int idx = 0; idx < cols; idx++) {
                    intColumns.set(idx, intCols[i & 511][idx]);
                    intRanges.set(idx, ranges[i & 511][idx]);
                }
                ZOrder.encodeIntegers(intColumns, intRanges);
            }
            time = System.currentTimeMillis() - time;
            totalTime += time;
            System.out.println(time);
        }
        System.out.println("Average time of " + runs + " runs: " + totalTime / runs);
    }

    @Test
    public void testZOrderEncodeTime()
            throws Exception
    {
        int runs = 10;
        int totalTime = 0;
        for (int x = 0; x < runs; x++) {
            long time = System.currentTimeMillis();
            List<Integer> intColumns = new ArrayList<>();
            intColumns.add(13);
            intColumns.add(97);
            intColumns.add(116);
            intColumns.add(15);
            intColumns.add(24);
            intColumns.add(108);
            intColumns.add(76);
            ZOrder.encodeBigIntegers(intColumns);
            Random rand = new Random();
            for (int i = 0; i < 10000000; i++) {
                for (int idx = 0; idx < 7; idx++) {
                    intColumns.set(idx, Math.abs(rand.nextInt()));
                    // intColumns.set(idx, rand.nextInt(128));
                }
                ZOrder.encodeBigIntegers(intColumns);
            }
            time = System.currentTimeMillis() - time;
            totalTime += time;
            System.out.println(time);
        }
        System.out.println("Average time of " + runs + " runs: " + totalTime / runs);
    }
}
