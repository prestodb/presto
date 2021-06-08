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
import java.util.List;

public final class ZOrder
{
    private ZOrder()
    {
    }

    public static int getHighestSetBit(int value)
    {
        int position = 0;
        while (value != 0) {
            value >>= 1;
            position++;
        }
        return position;
    }

    public static byte[] encodeIntegers(List<Integer> input)
    {
        int bitCollector = 0;
        for (int index = 0; index < input.size(); index++) {
            bitCollector |= input.get(index);
        }
        int highestSetBit = getHighestSetBit(bitCollector);
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

    public static List<Integer> decodeIntegers(int listLength, byte[] address)
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
