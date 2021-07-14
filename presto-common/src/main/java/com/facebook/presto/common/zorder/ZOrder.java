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

import com.facebook.presto.common.type.Type;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ZOrder
{
    private ZOrder()
    {
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

    public static byte[] encodeIntegers(List<Integer> input)
    {
        // Transfer into array for faster access and to change values
        int[] inputList = new int[input.size()];
        for (int index = 0; index < input.size(); index++) {
            inputList[index] = input.get(index);
        }

        int bitCollector = 0;
        for (int index = 0; index < input.size(); index++) {
            bitCollector |= inputList[index];
        }
        int highestSetBit = getHighestSetBitPosition(bitCollector);
        // Pad smaller numbers with 0s to same length as highestSetBit
        byte[] address = new byte[((input.size() * highestSetBit) >> 3) + 1];
        int bitIndex = 0;
        int bitOffset = 0;
        // Interweave input bits into address to preserve data locality
        for (int inputBitPos = 0; inputBitPos < highestSetBit; inputBitPos++) {
            for (int index = 0; index < input.size(); index++) {
                address[bitIndex >> 3] |= (inputList[index] & 1) << bitOffset;
                inputList[index] >>= 1;
                bitIndex++;
                bitOffset = bitIndex & 7;
            }
        }
        address[bitIndex >> 3] |= (byte) (1 << bitOffset);
        return address;
    }

    public static List<Integer> decodeIntegers(int listLength, byte[] address)
    {
        int[] output = new int[listLength];

        int highestSetBit = getHighestSetBitPosition(address[address.length - 1]) - 1;
        // Un-weave address into original integers
        int binaryLength = (((address.length - 1) << 3) + highestSetBit) / listLength;
        int byteIndex = 0;
        int bitOffset = 0;
        int bitIndex = 0;
        for (int outputBitPos = 0; outputBitPos < binaryLength; outputBitPos++) {
            for (int index = 0; index < listLength; index++) {
                output[index] |= ((address[byteIndex] >> bitOffset) & 1) << outputBitPos;
                bitIndex++;
                bitOffset = bitIndex & 7;
                byteIndex = bitIndex >> 3;
            }
        }

        List<Integer> intList = new ArrayList<>(output.length);
        for (int x : output) {
            intList.add(x);
        }
        return intList;
    }

    public static void printBits(byte[] bytes)
    {
        int byteCount = 0;
        for (byte b : bytes) {
            System.out.print(Integer.toBinaryString(Byte.toUnsignedInt(b)));
            byteCount++;
        }
        System.out.println();
        System.out.println(byteCount);
    }

    public static byte[] encodeIntegers(List<Integer> input, List<Integer> bitRanges)
    {
        // Transfer into array for faster access and to change values
        int[] inputList = new int[input.size()];
        for (int index = 0; index < input.size(); index++) {
            inputList[index] = input.get(index);
        }

        int totalBitLength = 0;
        int maxBitLength = 0;
        for (int bitLength : bitRanges) {
            totalBitLength += bitLength;
            maxBitLength = Math.max(maxBitLength, bitLength);
        }

        byte[] address = new byte[(totalBitLength + 7) >> 3];
        int bitIndex = totalBitLength - 1;
        int bitOffset = bitIndex & 7;
        // Interweave input bits into address from the most significant bit to preserve data locality
        for (int inputBitPos = 0; inputBitPos < maxBitLength; inputBitPos++) {
            for (int index = 0; index < input.size(); index++) {
                if (inputBitPos >= bitRanges.get(index)) {
                    continue;
                }
                int maskedBit = (inputList[index] >> (bitRanges.get(index) - inputBitPos - 1)) & 1;
                address[bitIndex >> 3] |= maskedBit << bitOffset;
                bitIndex--;
                bitOffset = bitIndex & 7;
            }
        }
        return address;
    }

    public static List<Integer> decodeIntegers(List<Integer> bitRanges, byte[] address)
    {
        int listLength = bitRanges.size();
        int[] output = new int[listLength];

        int totalBitLength = 0;
        int maxBitLength = 0;
        for (int bitLength : bitRanges) {
            totalBitLength += bitLength;
            maxBitLength = Math.max(maxBitLength, bitLength);
        }

        // Un-weave address into original integers
        int bitIndex = totalBitLength - 1;
        int bitOffset = bitIndex & 7;
        for (int outputBitPos = 0; outputBitPos < maxBitLength; outputBitPos++) {
            for (int index = 0; index < listLength; index++) {
                if (outputBitPos >= bitRanges.get(index)) {
                    continue;
                }
                int maskedBit = (address[bitIndex >> 3] >> bitOffset) & 1;
                output[index] |= maskedBit << (bitRanges.get(index) - outputBitPos - 1);
                bitIndex--;
                bitOffset = bitIndex & 7;
            }
        }

        List<Integer> intList = new ArrayList<>(output.length);
        for (int x : output) {
            intList.add(x);
        }
        return intList;
    }

    public static byte[] encodeIntegersBit(List<Integer> input)
    {
        int bitCollector = 0;
        for (int index = 0; index < input.size(); index++) {
            bitCollector |= input.get(index);
        }
        int highestSetBit = getHighestSetBitPosition(bitCollector);
        // Pad smaller numbers with 0s to the same length as maxBitLength
        byte[] address = new byte[highestSetBit * input.size()];
        int addressBitPos = 0;
        // Interweave input bits into address to preserve data locality
        for (int inputBitPos = 0; inputBitPos < highestSetBit; inputBitPos++) {
            for (int index = 0; index < input.size(); index++) {
                address[addressBitPos++] = (byte) ((input.get(index) >> inputBitPos) & 1);
            }
        }
        return address;
    }

    public static List<Integer> decodeIntegersBit(int listLength, byte[] address)
    {
        int[] output = new int[listLength];
        for (int index = 0; index < listLength; index++) {
            output[index] = 0;
        }

        // Un-weave address into original integers
        int binaryLength = address.length / listLength;
        int bitPosition = 0;
        for (int bitPos = 0; bitPos < binaryLength; bitPos++) {
            for (int index = 0; index < listLength; index++) {
                output[index] |= address[bitPosition++] << bitPos;
            }
        }

        List<Integer> intList = new ArrayList<>(output.length);
        for (int x : output) {
            intList.add(x);
        }
        return intList;
    }

    public static long encodeIntegersLongNeg(List<Integer> cols)
    {
        int colSize = cols.size();
        int maxValue = 0;
        for (int col = 0; col < colSize; col++) {
            int column = cols.get(col);
            if (column < 0) {
                column = -(column + 1);
            }
            maxValue = Math.max(maxValue, column);
        }

        int maxBitSize = getHighestSetBitPosition(maxValue);
        int bitPosition = 0;
        long address = 0;
        for (int bitPos = 0; bitPos < maxBitSize; bitPos++) {
            for (int col = 0; col < colSize; col++) {
                int column = cols.get(col);
                if (column < 0) {
                    column = -(column + 1);
                }
                address |= (long) ((column >> bitPos) & 1) << bitPosition++;
            }
        }
        for (int col = colSize - 1; col >= 0; col--) {
            int column = cols.get(col);
            if (column >= 0) {
                address |= (long) 1 << bitPosition;
            }
            bitPosition++;
        }

        address |= (long) 1 << bitPosition;

        return address;
    }

    public static List<Integer> decodeIntegersLongNeg(int colCount, long address)
    {
        Integer[] cols = new Integer[colCount];
        for (int col = 0; col < colCount; col++) {
            cols[col] = 0;
        }

        int binaryLength = getHighestSetBitPosition(address) - 1;
        int maxBitSize = (binaryLength - colCount) / colCount;
        int bitPosition = 0;
        for (int bitPos = 0; bitPos < maxBitSize; bitPos++) {
            for (int col = 0; col < colCount; col++) {
                cols[col] |= (int) ((address >> bitPosition++) & 1) << bitPos;
            }
        }

        for (int col = 0; col < colCount; col++) {
            boolean positive = ((address >> (binaryLength - col - 1)) & 1) == 1;
            if (!positive) {
                cols[col] = -cols[col] - 1;
            }
        }

        return new ArrayList<>(Arrays.asList(cols));
    }

    public static BigInteger encodeBigIntegers(List<Integer> cols)
    {
        int colSize = cols.size();
        int bitCounter = 0;
        for (int col = 0; col < colSize; col++) {
            int column = cols.get(col);
            bitCounter |= column;
        }

        int maxBitSize = getHighestSetBitPosition(bitCounter);
        int bitPosition = 0;
        BigInteger address = new BigInteger("0");
        for (int bitPos = 0; bitPos < maxBitSize; bitPos++) {
            for (int col = 0; col < colSize; col++) {
                int column = cols.get(col);
                int bit = ((column >> bitPos) & 1);
                if (bit == 1) {
                    address = address.setBit(bitPosition);
                }
                bitPosition++;
            }
        }

        return address;
    }

    public static List<Integer> decodeBigIntegers(int colCount, BigInteger address)
    {
        Integer[] cols = new Integer[colCount];
        for (int col = 0; col < colCount; col++) {
            cols[col] = 0;
        }

        int binaryLength = address.bitLength();
        int maxBitSize = (binaryLength + colCount - 1) / colCount;
        int bitPosition = 0;
        for (int bitPos = 0; bitPos < maxBitSize; bitPos++) {
            for (int col = 0; col < colCount; col++) {
                cols[col] |= (address.shiftRight(bitPosition++).intValue() & 1) << bitPos;
            }
        }

        return new ArrayList<>(Arrays.asList(cols));
    }

    public static byte[] encodeFloats(List<Float> input)
    {
        // Floats have 32-bit representations
        int maxBitLength = 32;
        byte[] address = new byte[maxBitLength * input.size()];
        int addressBitPos = 0;
        // Interweave float bits into address to preserve data locality
        for (int inputBitPos = 0; inputBitPos < maxBitLength; inputBitPos++) {
            for (int index = 0; index < input.size(); index++) {
                address[addressBitPos++] = (byte) ((Float.floatToRawIntBits(input.get(index)) >> inputBitPos) & 1);
            }
        }
        return address;
    }

    public static List<Float> decodeFloats(int listLength, byte[] address)
    {
        float[] output = new float[listLength];
        for (int index = 0; index < listLength; index++) {
            output[index] = 0;
        }

        // Un-weave address into original floats
        int binaryLength = address.length / listLength;
        int bitPosition = 0;
        for (int bitPos = 0; bitPos < binaryLength; bitPos++) {
            for (int index = 0; index < listLength; index++) {
                output[index] = Float.intBitsToFloat(Float.floatToRawIntBits(output[index]) | (address[bitPosition++] << bitPos));
            }
        }

        List<Float> floatList = new ArrayList<>(output.length);
        for (float x : output) {
            floatList.add(x);
        }
        return floatList;
    }

    public static long encodeStrings(List<Float> cols)
    {
        long address = 0;
        // Each character = 1 byte
        // Interweave numbers to create address
        return address;
    }

    public static long encodeCols(List<Object> cols, List<Type> types)
    {
        long address = 0;
        // Based on each type, create different binary representation
        // Interweave numbers to create address
        return address;
    }

    private static boolean isRelevant(BigInteger minAddress, BigInteger maxAddress, BigInteger address)
    {
        // Return whether the test address falls within the query rectangle
        // created by the minimum and maximum Z-addresses
        return true;
    }

    private static BigInteger nextJumpIn(BigInteger minAddress, BigInteger maxAddress, BigInteger address)
    {
        // Calculate the next smallest Z-address that is back inside the query rectangle
        return address;
    }

    public static List<BigInteger> searchCurve(List<Integer> lowestColValues, List<Integer> highestColValues)
    {
        BigInteger minAddress = new BigInteger(String.valueOf(encodeIntegers(lowestColValues)), 2);
        BigInteger maxAddress = new BigInteger(String.valueOf(encodeIntegers(highestColValues)), 2);
        // If one or more dimensions in a query are unbounded (for example,
        // no X range is specified), simply use the minimum and maximum
        // possible values for that dimension.
        List<BigInteger> addressList = new ArrayList<>();
        for (BigInteger address = minAddress; address.compareTo(maxAddress) <= 0; address = address.add(new BigInteger("1"))) {
            if (isRelevant(minAddress, maxAddress, address)) {
                addressList.add(address);
            }
            else {
                address = nextJumpIn(minAddress, maxAddress, address);
            }
        }
        return addressList;
    }
}
