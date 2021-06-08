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
package com.facebook.presto.hive.zorder;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestZOrder
{
    private static final long[][] EXPECTED_Z_ADDRESSES = {
            {0, 1, 4, 5, 16, 17, 20, 21},
            {2, 3, 6, 7, 18, 19, 22, 23},
            {8, 9, 12, 13, 24, 25, 28, 29},
            {10, 11, 14, 15, 26, 27, 30, 31},
            {32, 33, 36, 37, 48, 49, 52, 53},
            {34, 35, 38, 39, 50, 51, 54, 55},
            {40, 41, 44, 45, 56, 57, 60, 61},
            {42, 43, 46, 47, 58, 59, 62, 63}};

    @Test
    public void testZOrderNegativeIntegers()
    {
        ZOrder zOrder = new ZOrder(ImmutableList.of(8, 8, 8, 8, 8, 8));
        List<Integer> intColumns = ImmutableList.of(1, 2, 3, -10, 4, 5);

        try {
            zOrder.encodeToByteArray(intColumns);
            fail("Expected test to fail: z-ordering does not support negative integers.");
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = "Current Z-Ordering implementation does not support negative input numbers.";
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderDifferentListSizes()
    {
        List<Integer> bitPositions = ImmutableList.of(8, 8, 8, 8, 8, 8, 8, 8);
        ZOrder zOrder = new ZOrder(bitPositions);
        List<Integer> intColumns = ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        try {
            zOrder.encodeToByteArray(intColumns);
            fail("Expected test to fail: input list size is greater than bit position list size.");
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = format(
                    "Input list size (%d) does not match encoding bits list size (%d).",
                    intColumns.size(),
                    bitPositions.size());
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderTooManyIntegers()
    {
        Random rand = new Random();
        int listLength = ZOrder.MAX_INPUT_DIMENSIONS + 1;

        List<Integer> intColumns = new ArrayList<>(listLength);
        List<Integer> bitPositions = new ArrayList<>(listLength);
        for (int i = 0; i < listLength; i++) {
            int value = rand.nextInt(Integer.MAX_VALUE);
            intColumns.add(value);
            bitPositions.add(getHighestSetBitPosition(value) + 1);
        }

        ZOrder zOrder = new ZOrder(bitPositions);

        try {
            zOrder.encodeToByteArray(intColumns);
            fail(format("Expected test to fail: z-ordering does not support more than %d integers.", ZOrder.MAX_INPUT_DIMENSIONS));
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = format("Current Z-Ordering implementation does not support more than %d input numbers.", ZOrder.MAX_INPUT_DIMENSIONS);
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderNullInput()
    {
        ZOrder zOrder = new ZOrder(ImmutableList.of(8, 8));

        try {
            zOrder.encodeToByteArray(null);
            fail("Expected test to fail: input list should not be null.");
        }
        catch (NullPointerException e) {
            String expectedMessage = "Input list should not be null.";
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderEmptyInput()
    {
        ZOrder zOrder = new ZOrder(ImmutableList.of(8, 8));
        List<Integer> intColumns = ImmutableList.of();

        try {
            zOrder.encodeToByteArray(intColumns);
            fail("Expected test to fail: input size should be greater than zero.");
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = "Input list size should be greater than zero.";
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderNullEncodingBits()
    {
        try {
            new ZOrder(null);
            fail("Expected test to fail: encoding bits list should not be null.");
        }
        catch (NullPointerException e) {
            String expectedMessage = "Encoding bits list should not be null.";
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderEmptyEncodingBits()
    {
        try {
            new ZOrder(ImmutableList.of());
            fail("Expected test to fail: encoding bits list should not be empty.");
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = "Encoding bits list should not be empty.";
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderToByteArray()
    {
        ZOrder zOrder = new ZOrder(ImmutableList.of(3, 3));

        for (int x = 0; x < 8; x++) {
            for (int y = 0; y < 8; y++) {
                List<Integer> intColumns = ImmutableList.of(x, y);

                byte[] byteAddress = zOrder.encodeToByteArray(intColumns);

                long address = zOrder.zOrderByteAddressToLong(byteAddress);
                assertEquals(address, EXPECTED_Z_ADDRESSES[x][y]);

                List<Integer> decodedIntCols = zOrder.decode(address);
                assertEquals(intColumns, decodedIntCols, "Integers decoded improperly");
            }
        }
    }

    @Test
    public void testZOrderToLong()
    {
        ZOrder zOrder = new ZOrder(ImmutableList.of(3, 3));

        for (int x = 0; x < 8; x++) {
            for (int y = 0; y < 8; y++) {
                List<Integer> intColumns = ImmutableList.of(x, y);

                long address = zOrder.encodeToLong(intColumns);
                assertEquals(address, EXPECTED_Z_ADDRESSES[x][y]);

                List<Integer> decodedIntCols = zOrder.decode(address);
                assertEquals(intColumns, decodedIntCols, "Integers decoded improperly");
            }
        }
    }

    @Test
    public void testZOrderToInt()
    {
        ZOrder zOrder = new ZOrder(ImmutableList.of(3, 3));

        for (int x = 0; x < 8; x++) {
            for (int y = 0; y < 8; y++) {
                List<Integer> intColumns = ImmutableList.of(x, y);

                int address = zOrder.encodeToInteger(intColumns);
                assertEquals(address, EXPECTED_Z_ADDRESSES[x][y]);

                List<Integer> decodedIntCols = zOrder.decode(address);
                assertEquals(intColumns, decodedIntCols, "Integers decoded improperly");
            }
        }
    }

    @Test
    public void testZOrderOverLong()
    {
        List<Integer> bitPositions = ImmutableList.of(16, 16, 16, 16, 16);
        int totalBitLength = bitPositions.stream().mapToInt(Integer::intValue).sum();
        ZOrder zOrder = new ZOrder(bitPositions);
        List<Integer> intColumns = ImmutableList.of(20456, 20456, 20456, 20456, 20456);

        try {
            zOrder.encodeToLong(intColumns);
            fail("Expected test to fail: total bits to encode is larger than the size of a long.");
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = format("The z-address type specified is not large enough to hold %d bits.", totalBitLength);
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    @Test
    public void testZOrderOverInt()
    {
        List<Integer> bitPositions = ImmutableList.of(16, 16, 16);
        int totalBitLength = bitPositions.stream().mapToInt(Integer::intValue).sum();
        ZOrder zOrder = new ZOrder(bitPositions);
        List<Integer> intColumns = ImmutableList.of(20456, 20456, 20456);

        try {
            zOrder.encodeToInteger(intColumns);
            fail("Expected test to fail: total bits to encode is larger than the size of a integer.");
        }
        catch (IllegalArgumentException e) {
            String expectedMessage = format("The z-address type specified is not large enough to hold %d bits.", totalBitLength);
            assertEquals(e.getMessage(), expectedMessage, format("Expected exception message '%s' to match '%s'", e.getMessage(), expectedMessage));
        }
    }

    private static int getHighestSetBitPosition(int value)
    {
        // Assumes value is non-negative
        int position = 0;
        while (value != 0) {
            value >>= 1;
            position++;
        }
        return position;
    }
}
