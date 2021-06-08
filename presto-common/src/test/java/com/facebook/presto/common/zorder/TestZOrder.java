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
        List<Integer> intColumns = new ArrayList<>();
        intColumns.add(13453);
        intColumns.add(943547);
        intColumns.add(7502919);
        intColumns.add(57834567);
        intColumns.add(0);
        intColumns.add(198);
        byte[] address1 = ZOrder.encodeIntegers(intColumns);
        // System.out.println(Long.toBinaryString(address1.longValue()) + ": " + address1);
        List<Integer> decodedCols = ZOrder.decodeIntegers(intColumns.size(), address1);
        System.out.println(intColumns.toString());
        System.out.println(decodedCols.toString());
        // long address2 = ZOrder.encodeIntegers(intColumns);
        // System.out.println(Long.toBinaryString(address2) + ": " + address2);
    }

    @Test
    public void testZOrderTime()
            throws Exception
    {
        for (int x = 0; x < 3; x++) {
            long time = System.currentTimeMillis();
            List<Integer> intColumns = new ArrayList<>();
            intColumns.add(13);
            intColumns.add(97);
            intColumns.add(116);
            intColumns.add(15);
            intColumns.add(24);
            intColumns.add(108);
            intColumns.add(76);
            byte[] address = ZOrder.encodeIntegers(intColumns);
            List<Integer> decodedIntCols = ZOrder.decodeIntegers(7, address);
            // System.out.println(Long.toBinaryString(address) + ": " + address);
            if (!intColumns.toString().equals(decodedIntCols.toString())) {
                throw new Exception("Integers decoded improperly");
            }
            Random rand = new Random();
            for (int i = 0; i < 10000000; i++) {
                for (int idx = 0; idx < 7; idx++) {
                    intColumns.set(idx, Math.abs(rand.nextInt()) % 128);
                }
                address = ZOrder.encodeIntegers(intColumns);
                decodedIntCols = ZOrder.decodeIntegers(7, address);
                // System.out.println(Long.toBinaryString(address) + ": " + address);
                if (!intColumns.toString().equals(decodedIntCols.toString())) {
                    System.out.println(intColumns.toString());
                    System.out.println(decodedIntCols.toString());
                    throw new Exception("Integers decoded improperly");
                }
            }
            time = System.currentTimeMillis() - time;
            System.out.println(time);
            if (time > 100000) {
                throw new Exception("Encoding+decoding too slow");
            }
        }
    }

    @Test
    public void testZOrderEncodeTime()
            throws Exception
    {
        for (int x = 0; x < 10; x++) {
            long time = System.currentTimeMillis();
            List<Integer> intColumns = new ArrayList<>();
            intColumns.add(13);
            intColumns.add(97);
            intColumns.add(116);
            intColumns.add(15);
            intColumns.add(24);
            intColumns.add(108);
            intColumns.add(76);
            ZOrder.encodeIntegers(intColumns);
            Random rand = new Random();
            for (int i = 0; i < 1000000; i++) {
                for (int idx = 0; idx < 7; idx++) {
                    intColumns.set(idx, rand.nextInt(128));
                }
                ZOrder.encodeIntegers(intColumns);
            }
            time = System.currentTimeMillis() - time;
            System.out.println(time);
            if (time > 100000) {
                throw new Exception("Encoding+decoding too slow");
            }
        }
    }
}
