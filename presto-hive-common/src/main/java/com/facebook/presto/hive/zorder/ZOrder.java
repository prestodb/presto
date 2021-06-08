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

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The ZOrder class provides functions to map data from multiple columns of a
 * table to a single dimensional z-address (encode) and vice versa (decode),
 * while preserving data locality for multidimensional value combinations.
 */
public class ZOrder
{
    public static final int MAX_INPUT_DIMENSIONS = 10;

    // Assume encodingBits is 1-based
    private final List<Integer> encodingBits;
    private final int totalBitLength;
    private final int maxBitLength;

    /**
     * Class constructor specifying the number of bits each value will take up for encoding and decoding.
     */
    public ZOrder(List<Integer> encodingBits)
    {
        requireNonNull(encodingBits, "Encoding bits list should not be null.");
        checkArgument(!encodingBits.isEmpty(), "Encoding bits list should not be empty.");

        this.encodingBits = encodingBits;

        totalBitLength = encodingBits.stream().mapToInt(Integer::intValue).sum();
        maxBitLength = encodingBits.stream().mapToInt(Integer::intValue).max().getAsInt();
    }

    /**
     * Encodes the input list into its corresponding z-address and returns a byte array, with the least significant bit at index 0.
     * <p/>
     * <code>encode</code> interweaves the bit representations of the input values from the most significant bit to create the final z-address.
     * <p/>
     * For example: for a list of (7, 128) = (0b00000111, 0b10000000), <code>encode</code> returns a z-address of 16426 = 0b0100000000101010.
     *
     * @param input the list of integer values to be encoded
     * @return the byte array representation of the z-address
     */
    public byte[] encodeToByteArray(List<Integer> input)
    {
        checkEncodeInputValidity(input);

        // Find address byte length by rounding up (totalBitLength / 8)
        byte[] address = new byte[(totalBitLength + 7) >> 3];
        int bitIndex = totalBitLength - 1;
        // Interweave input bits into address from the most significant bit to preserve data locality
        for (int bitsProcessed = 0; bitsProcessed < maxBitLength; bitsProcessed++) {
            for (int index = 0; index < input.size(); index++) {
                if (bitsProcessed >= encodingBits.get(index)) {
                    continue;
                }
                int bitPosition = encodingBits.get(index) - bitsProcessed - 1;
                byte maskedBit = (byte) ((input.get(index) >> bitPosition) & 1);
                address[bitIndex >> 3] |= maskedBit << (bitIndex & 7);
                bitIndex--;
            }
        }
        return address;
    }

    /**
     * Encodes the input list into its corresponding z-address and returns a long.
     *
     * @param input the list of integer values to be encoded
     * @return the long representation of the z-address
     */
    public long encodeToLong(List<Integer> input)
    {
        checkEncodeInputValidity(input, Long.SIZE);

        return zOrderByteAddressToLong(encodeToByteArray(input));
    }

    /**
     * Encodes the input list into its corresponding z-address and returns an integer.
     *
     * @param input the list of integer values to be encoded
     * @return the integer representation of the z-address
     */
    public int encodeToInteger(List<Integer> input)
    {
        checkEncodeInputValidity(input, Integer.SIZE);

        return (int) zOrderByteAddressToLong(encodeToByteArray(input));
    }

    /**
     * Decodes the z-address into its corresponding input list.
     * <p/>
     * For example: for a z-address of 16426 = 0b0100000000101010, <code>decode</code> returns a list of (7, 128) = (0b00000111, 0b10000000).
     *
     * @param address the byte address representation of the z-address
     * @return the list of integer values that were encoded
     */
    public List<Integer> decode(byte[] address)
    {
        int[] output = new int[encodingBits.size()];

        int bitIndex = totalBitLength - 1;
        int bitsProcessed = 0;
        // Un-weave address into original integers
        while (bitIndex >= 0) {
            for (int index = 0; index < output.length; index++) {
                if (bitsProcessed >= encodingBits.get(index)) {
                    continue;
                }
                byte maskedBit = (byte) ((address[bitIndex >> 3] >> (bitIndex & 7)) & 1);
                int bitPosition = encodingBits.get(index) - bitsProcessed - 1;
                output[index] |= maskedBit << bitPosition;
                bitIndex--;
            }
            bitsProcessed++;
        }

        return Arrays.stream(output).boxed().collect(ImmutableList.toImmutableList());
    }

    /**
     * Decodes the z-address into its corresponding input list.
     *
     * @param address the long representation of the z-address
     * @return the list of integer values that were encoded
     */
    public List<Integer> decode(long address)
    {
        return decode(zOrderLongToByteAddress(address));
    }

    /**
     * Decodes the z-address into its corresponding input list.
     *
     * @param address the integer representation of the z-address
     * @return the list of integer values that were encoded
     */
    public List<Integer> decode(int address)
    {
        return decode((long) address);
    }

    /**
     * Converts the byte array representation of a z-address into its long representation.
     *
     * @param byteAddress the byte array representation of the z-address
     * @return the long representation of the z-address
     */
    public long zOrderByteAddressToLong(byte[] byteAddress)
    {
        long address = 0;
        for (int byteIndex = 0; byteIndex < byteAddress.length; byteIndex++) {
            address |= Byte.toUnsignedLong(byteAddress[byteIndex]) << (byteIndex << 3);
        }
        return address;
    }

    /**
     * Converts the long representation of a z-address into its byte array representation.
     *
     * @param address the long representation of the z-address
     * @return the byte array representation of the z-address
     */
    public byte[] zOrderLongToByteAddress(long address)
    {
        byte[] byteAddress = new byte[(totalBitLength + 7) >> 3];
        for (int bitIndex = 0; bitIndex < totalBitLength; bitIndex++) {
            byteAddress[bitIndex >> 3] |= ((address >> bitIndex) & 1) << (bitIndex & 7);
        }
        return byteAddress;
    }

    /**
     * Check if the input list has a valid size and only contains positive values.
     *
     * @param input the list of integer values to be encoded
     */
    private void checkEncodeInputValidity(List<Integer> input)
    {
        requireNonNull(input, "Input list should not be null.");

        checkArgument(!input.isEmpty(), "Input list size should be greater than zero.");

        checkArgument((input.size() <= MAX_INPUT_DIMENSIONS), format(
                "Current Z-Ordering implementation does not support more than %d input numbers.",
                MAX_INPUT_DIMENSIONS));

        checkArgument((input.size() == encodingBits.size()), format(
                "Input list size (%d) does not match encoding bits list size (%d).",
                input.size(),
                encodingBits.size()));

        input.forEach(value -> checkArgument(value >= 0, "Current Z-Ordering implementation does not support negative input numbers."));
    }

    /**
     * Checks if the input list is valid and the requested return type is large enough to store the z-address.
     *
     * @param input the list of integer values to be encoded
     * @param maximumBits the maximum amount of bits supported by the requested return type
     */
    private void checkEncodeInputValidity(List<Integer> input, int maximumBits)
    {
        checkEncodeInputValidity(input);

        checkArgument((totalBitLength <= maximumBits), format("The z-address type specified is not large enough to hold %d bits.", totalBitLength));
    }
}
