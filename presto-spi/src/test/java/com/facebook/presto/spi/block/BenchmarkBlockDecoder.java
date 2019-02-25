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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.Page;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Scanner;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;

//import com.facebook.presto.spi.type.BigintType;
//import com.facebook.presto.spi.type.DoubleType;
//import static org.testng.Assert.assertEquals;

public class BenchmarkBlockDecoder
{
    // Evaluator class that would be generated from
    // extendedprice * (1 - discount) - quantity * supplycost
    public static class ProfitExpr
    {
        BlockDecoder extendedPrice = new BlockDecoder();
        BlockDecoder discount = new BlockDecoder();
        BlockDecoder quantity = new BlockDecoder();
        BlockDecoder supplyCost = new BlockDecoder();
        boolean[] nullsInReserve;
        boolean[] nullsInBatch;
        long[][] tempLongs = new long[1][];
        double[][] tempDoubles = new double[1][];

        static Unsafe unsafe;

        static {
            try {
                // fetch theUnsafe object
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafe = (Unsafe) field.get(null);
                if (unsafe == null) {
                    throw new RuntimeException("Unsafe access not available");
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        double[] asDoubleArray(long[] longs)
        {
            tempLongs[0] = longs;
            //Slice source = new Slice(tempLongs, 0, 24, 24, tempLongs);
            //Slice target = new Slice(tempLongs, 0, 24, 24, tempLongs);
            //target.putLong(16, source.getLong(16));
            unsafe.copyMemory(tempLongs, 16, tempDoubles, 16, 8);
            return tempDoubles[0];
        }

        void boolArrayOr(boolean[] target, boolean[] source, int[] map, int positionCount)
        {
            if (map == null) {
                int i = 0;
	  /*
	  int bytesInWords = positionCount & ~7;
	  for (; i < bytesInWords; i += 8) {
	      unsafe.putLong(target, 16, unsafe.getLong(target, 16 + i) |
			     unsafe.getLong(source, 16 + i));
	  }
	  */
                for (; i < positionCount; ++i) {
                    target[i] |= source[i];
                }
            }
            else {
                for (int i = 0; i < positionCount; ++i) {
                    target[i] |= source[map[i]];
                }
            }
        }

        void addNullFlags(boolean[] nullFlags, int[] map, int positionCount)
        {
            if (nullFlags != null) {
                if (nullsInBatch == null && map == null) {
                    nullsInBatch = nullFlags;
                }
                else {
                    boolean[] newNulls;
                    if (nullsInReserve != null && nullsInReserve.length >= positionCount) {
                        newNulls = nullsInReserve;
                    }
                    else {
                        newNulls = new boolean[positionCount];
                        nullsInReserve = newNulls;
                    }
                    if (nullsInBatch != null) {
                        System.arraycopy(nullsInBatch, 0, newNulls, 0, positionCount);
                    }
                    nullsInBatch = newNulls;
                    boolArrayOr(nullsInBatch, nullFlags, map, positionCount);
                }
            }
        }

        Page Evaluate(Page page)
        {
            int positionCount = page.getPositionCount();
            extendedPrice.decodeBlock(page.getBlock(0));
            discount.decodeBlock(page.getBlock(1));
            quantity.decodeBlock(page.getBlock(2));
            supplyCost.decodeBlock(page.getBlock(3));
            long[] ep = extendedPrice.getValues(long[].class);
            long[] di = discount.getValues(long[].class);
            long[] qt = quantity.getValues(long[].class);
            long[] sc = supplyCost.getValues(long[].class);
            int[] epMap = extendedPrice.getRowNumberMap();
            int[] diMap = discount.getRowNumberMap();
            int[] qtMap = quantity.getRowNumberMap();
            int[] scMap = supplyCost.getRowNumberMap();
            long[] result = new long[positionCount];
            nullsInBatch = null;
            addNullFlags(extendedPrice.getValueIsNull(), extendedPrice.isIdentityMap() ? null : epMap, positionCount);
            addNullFlags(discount.getValueIsNull(), discount.isIdentityMap() ? null : diMap, positionCount);
            addNullFlags(quantity.getValueIsNull(), quantity.isIdentityMap() ? null : qtMap, positionCount);
            addNullFlags(supplyCost.getValueIsNull(), supplyCost.isIdentityMap() ? null : scMap, positionCount);
            boolean allIdentity = extendedPrice.isIdentityMap() || discount.isIdentityMap() | quantity.isIdentityMap() | supplyCost.isIdentityMap();
            if (allIdentity && nullsInBatch == null) {
                for (int i = 0; i < positionCount; ++i) {
                    result[i] = doubleToLongBits(longBitsToDouble(ep[i]) * (1 - longBitsToDouble(di[i])) - qt[i] * longBitsToDouble(sc[i]));
                }
            }
            else {
                for (int i = 0; i < positionCount; ++i) {
                    if (nullsInBatch == null || !nullsInBatch[i]) {
                        result[i] = doubleToLongBits(longBitsToDouble(ep[epMap[i]]) * (1 - longBitsToDouble(di[diMap[i]])) - qt[qtMap[i]] * longBitsToDouble(sc[scMap[i]]));
                    }
                }
            }
            extendedPrice.release();
            discount.release();
            quantity.release();
            supplyCost.release();
            return new Page(positionCount, new LongArrayBlock(0, positionCount, (nullsInBatch != null ? nullsInBatch.clone() : null), result));
        }

        Page EvaluateDouble(Page page)
        {
            int positionCount = page.getPositionCount();
            extendedPrice.decodeBlock(page.getBlock(0));
            discount.decodeBlock(page.getBlock(1));
            quantity.decodeBlock(page.getBlock(2));
            supplyCost.decodeBlock(page.getBlock(3));
            double[] ep = extendedPrice.getValues(double[].class);
            double[] di = discount.getValues(double[].class);
            long[] qt = quantity.getValues(long[].class);
            double[] sc = supplyCost.getValues(double[].class);
            int[] epMap = extendedPrice.getRowNumberMap();
            int[] diMap = discount.getRowNumberMap();
            int[] qtMap = quantity.getRowNumberMap();
            int[] scMap = supplyCost.getRowNumberMap();
            double[] result = new double[positionCount];
            nullsInBatch = null;
            addNullFlags(extendedPrice.getValueIsNull(), extendedPrice.isIdentityMap() ? null : epMap, positionCount);
            addNullFlags(discount.getValueIsNull(), discount.isIdentityMap() ? null : diMap, positionCount);
            addNullFlags(quantity.getValueIsNull(), quantity.isIdentityMap() ? null : qtMap, positionCount);
            addNullFlags(supplyCost.getValueIsNull(), supplyCost.isIdentityMap() ? null : scMap, positionCount);
            boolean allIdentity = extendedPrice.isIdentityMap() || discount.isIdentityMap() | quantity.isIdentityMap() | supplyCost.isIdentityMap();
            if (allIdentity && nullsInBatch == null) {
                for (int i = 0; i < positionCount; ++i) {
                    result[i] = (ep[i]) * (1 - (di[i])) - qt[i] * (sc[i]);
                }
            }
            else {
                for (int i = 0; i < positionCount; ++i) {
                    if (nullsInBatch == null || !nullsInBatch[i]) {
                        result[i] = (ep[epMap[i]]) * (1 - (di[diMap[i]])) - qt[qtMap[i]] * (sc[scMap[i]]);
                    }
                }
            }
            extendedPrice.release();
            discount.release();
            quantity.release();
            supplyCost.release();
            return new Page(positionCount, new DoubleArrayBlock(0, positionCount, (nullsInBatch != null ? nullsInBatch.clone() : null), result));
        }

        Page EvaluateRow(Page page)
        {
            Block ep = page.getBlock(0);
            Block di = page.getBlock(1);
            Block qt = page.getBlock(2);
            Block sc = page.getBlock(3);
            int positionCount = page.getPositionCount();
            LongArrayBlockBuilder res = new LongArrayBlockBuilder(null, positionCount);
            for (int i = 0; i < positionCount; ++i) {
                if (ep.isNull(i) || di.isNull(i) || qt.isNull(i) || sc.isNull(i)) {
                    res.appendNull();
                }
                else {
                    res.writeLong(
                            doubleToLongBits(
                                    longBitsToDouble(ep.getLong(i, 0)) * (1 - longBitsToDouble(di.getLong(i, 0))) - qt.getLong(i, 0) * longBitsToDouble(sc.getLong(i, 0))));
                }
            }
            return new Page(positionCount, res.build());
        }
    }

    static class DataSource
    {
        Page ownedPage;

        static Block addDict(boolean isReverse, Block block)
        {
            int positionCount = block.getPositionCount();
            int[] ids = new int[positionCount];
            for (int i = 0; i < positionCount; ++i) {
                ids[i] = isReverse ? positionCount - i - 1 : i;
            }
            return new DictionaryBlock(block, ids);
        }

        public Page nextPage(int numberOfRows, boolean addNulls, boolean addDicts)
        {
            LongArrayBlockBuilder ep = new LongArrayBlockBuilder(null, numberOfRows);
            LongArrayBlockBuilder di = new LongArrayBlockBuilder(null, numberOfRows);
            LongArrayBlockBuilder qt = new LongArrayBlockBuilder(null, numberOfRows);
            LongArrayBlockBuilder sc = new LongArrayBlockBuilder(null, numberOfRows);
            for (int i = 0; i < numberOfRows; ++i) {
                if (addNulls && i % 17 == 0) {
                    ep.appendNull();
                }
                else {
                    ep.writeLong(doubleToLongBits((double) i * 10));
                }
                if (addNulls && i % 21 == 0) {
                    di.appendNull();
                }
                else {
                    di.writeLong(doubleToLongBits((double) i % 10));
                }
                if (addNulls && i % 31 == 0) {
                    qt.appendNull();
                }
                else {
                    qt.writeLong(1 + (i % 50));
                }
                if (addNulls && i % 41 == 0) {
                    sc.appendNull();
                }
                else {
                    sc.writeLong(doubleToLongBits(1 + (double) i * 9));
                }
            }
            if (addDicts) {
                return new Page(numberOfRows, ep.build(), addDict(false, di.build()), addDict(true, addDict(false, qt.build())), addDict(true, addDict(false, addDict(true, sc.build()))));
            }
            else {
                return new Page(numberOfRows, ep.build(), di.build(), qt.build(), sc.build());
            }
        }

        public Page nextPageDouble(int numberOfRows, boolean addNulls, boolean addDicts)
        {
            DoubleArrayBlockBuilder ep = new DoubleArrayBlockBuilder(null, numberOfRows);
            DoubleArrayBlockBuilder di = new DoubleArrayBlockBuilder(null, numberOfRows);
            LongArrayBlockBuilder qt = new LongArrayBlockBuilder(null, numberOfRows);
            DoubleArrayBlockBuilder sc = new DoubleArrayBlockBuilder(null, numberOfRows);
            for (int i = 0; i < numberOfRows; ++i) {
                if (addNulls && i % 17 == 0) {
                    ep.appendNull();
                }
                else {
                    ep.writeDouble(((double) i * 10));
                }
                if (addNulls && i % 21 == 0) {
                    di.appendNull();
                }
                else {
                    di.writeDouble(((double) i % 10));
                }
                if (addNulls && i % 31 == 0) {
                    qt.appendNull();
                }
                else {
                    qt.writeLong(1 + (i % 50));
                }
                if (addNulls && i % 41 == 0) {
                    sc.appendNull();
                }
                else {
                    sc.writeDouble((1 + (double) i * 9));
                }
            }
            if (addDicts) {
                return new Page(numberOfRows, ep.build(), addDict(false, di.build()), addDict(true, addDict(false, qt.build())), addDict(true, addDict(false, addDict(true, sc.build()))));
            }
            else {
                return new Page(numberOfRows, ep.build(), di.build(), qt.build(), sc.build());
            }
        }
    }

    public static void TestCase(DataSource source, ProfitExpr expr, boolean nulls, boolean dicts)
    {
        System.out.println("===" + (nulls ? " with nulls " : " no nulls") + (dicts ? " nested dicts " : " flat "));
        Page page = source.nextPage(1000, nulls, dicts);
        Page pageDouble = source.nextPageDouble(1000, nulls, dicts);
        long tim = System.currentTimeMillis();
        for (int i = 0; i < 100000; ++i) {
            Page res = expr.Evaluate(page);
        }
        long endtim = System.currentTimeMillis();
        System.out.println("===vec long: " + (endtim - tim));
        tim = System.currentTimeMillis();
        for (int i = 0; i < 100000; ++i) {
            Page res = expr.EvaluateDouble(pageDouble);
        }
        endtim = System.currentTimeMillis();
        System.out.println("===vec double: " + (endtim - tim));
        tim = System.currentTimeMillis();
        for (int i = 0; i < 100000; ++i) {
            //Page page = source.nextPage(1000, false);
            Page res = expr.EvaluateRow(page);
        }
        endtim = System.currentTimeMillis();
        System.out.println("===blocks in loop: " + (endtim - tim));
    }

    public static void TestExpr()
    {
        DataSource source = new DataSource();
        ProfitExpr expr = new ProfitExpr();
        for (int i = 0; i < 11000; ++i) {
            Page page = source.nextPage(1, false, false);
            Page pageDouble = source.nextPageDouble(1, false, false);
            Page res = expr.Evaluate(page);
            res = expr.EvaluateDouble(pageDouble);
            res = expr.EvaluateRow(page);
        }
        for (int ctr = 0; ctr < 1; ++ctr) {
            TestCase(source, expr, false, false);
            TestCase(source, expr, false, true);
            TestCase(source, expr, true, false);
            TestCase(source, expr, true, true);
        }
    }

    public void RunTestExpr()
    {
        System.out.println("Test Runner:");
        TestExpr();
    }

    public static void testSlice()
    {
        Slice slice = Slices.allocateDirect(2000030);
        long sum = 0;
        for (int ctr = 0; ctr < 10000; ctr++) {
            for (int i = 0; i < 2000000; ++i) {
                sum += testMem(slice, i);
            }
        }
        System.out.println("Sum " + sum);
    }

    static long testMem(Slice slice, int off)
    {
        slice.setLong(off + 10, 11);
        slice.setLong(off + 18, 11);
        return slice.getLong(off + 10) + slice.getLong(off + 18);
    }

    public static void main(String args[])
            throws InterruptedException
    {
        System.out.println("===Main:");
        testSlice();
//        TestExpr();
        System.out.println("Press enter");
        new Scanner(System.in).nextLine();
//        TestExpr();
        testSlice();
    }
}
