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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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

    // Assume encodingBits is 0-based
    private final List<Integer> encodingBits;
    private final int totalBitLength;
    private final int maxBitLength;

    private final int[] dimensions;

    /**
     * Class constructor specifying the number of bits each value will take up for encoding and decoding.
     */
    public ZOrder(List<Integer> encodingBits)
    {
        requireNonNull(encodingBits, "Encoding bits list should not be null.");
        checkArgument(!encodingBits.isEmpty(), "Encoding bits list should not be empty.");

        this.encodingBits = encodingBits;

        totalBitLength = encodingBits.stream().mapToInt(Integer::intValue).sum() + encodingBits.size();
        maxBitLength = encodingBits.stream().mapToInt(Integer::intValue).max().getAsInt() + 1;

        dimensions = initializeDimensions();
    }

    /**
     * Initializes <code>dimensions</code> array, which stores how many values are stored at each level of the Z-curve.
     * <p/>
     * For example, given <code>encodingBits</code> = [2, 8, 6] and <code>maxBitLength</code> = 8, <code>dimensions</code> = [3, 3, 2, 2, 2, 2, 1, 1].
     */
    private int[] initializeDimensions()
    {
        int[] dimensions = new int[maxBitLength];
        List<Integer> bitLengths = new ArrayList<>(encodingBits);
        for (int dimensionIndex = maxBitLength - 1; dimensionIndex >= 0; dimensionIndex--) {
            for (int bitLengthIndex = 0; bitLengthIndex < bitLengths.size(); bitLengthIndex++) {
                if (bitLengths.get(bitLengthIndex) >= 0) {
                    dimensions[dimensionIndex]++;
                    bitLengths.set(bitLengthIndex, bitLengths.get(bitLengthIndex) - 1);
                }
            }
        }
        return dimensions;
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

        // Modify sign bits to preserve ordering between positive and negative integers
        int bitIndex = totalBitLength - 1;
        for (int value : input) {
            byte signBit = (value < 0) ? (byte) 0 : 1;
            address[bitIndex >> 3] |= signBit << (bitIndex & 7);
            bitIndex--;
        }

        bitIndex = totalBitLength - encodingBits.size() - 1;
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

        int bitIndex = totalBitLength - encodingBits.size() - 1;
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

        // Set correct sign bit for outputs
        bitIndex = totalBitLength - 1;
        for (int index = 0; index < output.length; index++) {
            byte signBit = (byte) ((address[bitIndex >> 3] >> (bitIndex & 7)) & 1);
            if (signBit == 0) {
                output[index] -= (1 << encodingBits.get(index));
            }
            bitIndex--;
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
        byte[] byteAddress = new byte[(totalBitLength + 7 + encodingBits.size()) >> 3];
        for (int bitIndex = 0; bitIndex < totalBitLength + encodingBits.size(); bitIndex++) {
            byteAddress[bitIndex >> 3] |= ((address >> bitIndex) & 1) << (bitIndex & 7);
        }
        return byteAddress;
    }

    /**
     * Searches for and outputs ranges of long addresses within certain ranges in each dimension.
     */
    public List<ZAddressRange<Long>> zOrderSearchCurveLongs(List<ZValueRange> ranges)
    {
        return zOrderSearchCurve(ranges);
    }

    /**
     * Searches for and outputs ranges of integer addresses within certain ranges in each dimension.
     */
    public List<ZAddressRange<Integer>> zOrderSearchCurveIntegers(List<ZValueRange> ranges)
    {
        List<ZAddressRange<Long>> addressRanges = zOrderSearchCurve(ranges);

        List<ZAddressRange<Integer>> integerAddressRanges = new ArrayList<>();
        for (ZAddressRange<Long> addressRange : addressRanges) {
            checkArgument(
                    (addressRange.getMinimumAddress() <= Integer.MAX_VALUE) && (addressRange.getMaximumAddress() <= Integer.MAX_VALUE),
                    format("The address range [%d, %d] contains addresses greater than integers.", addressRange.getMinimumAddress(), addressRange.getMaximumAddress()));

            integerAddressRanges.add(new ZAddressRange<>(addressRange.getMinimumAddress().intValue(), addressRange.getMaximumAddress().intValue()));
        }
        return integerAddressRanges;
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

        checkArgument(
                input.size() <= MAX_INPUT_DIMENSIONS,
                format("Current Z-Ordering implementation does not support more than %d input numbers.", MAX_INPUT_DIMENSIONS));

        checkArgument(
                input.size() == encodingBits.size(),
                format("Input list size (%d) does not match encoding bits list size (%d).", input.size(), encodingBits.size()));

        for (int i = 0; i < input.size(); i++) {
            checkArgument(
                    input.get(i) >= 0 ? (input.get(i) < (1 << encodingBits.get(i))) : (input.get(i) >= -(1 << encodingBits.get(i))),
                    format("Input value %d at index %d should not have more than %d bits.", input.get(i), i, encodingBits.get(i)));
        }
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

        checkArgument(
                totalBitLength <= maximumBits,
                format("The z-address type specified is not large enough to hold %d values with a total of %d bits.", encodingBits.size(), totalBitLength - encodingBits.size()));
    }

    private List<ZAddressRange<Long>> zOrderSearchCurve(List<ZValueRange> valueRanges)
    {
        List<ZAddressRange<Long>> addressRanges = new ArrayList<>();

        findAddressesRecursively(maxBitLength - 1, 0L, totalBitLength, fillUnspecifiedRangesWithDefaults(valueRanges), addressRanges);

        return combineAddressRanges(addressRanges);
    }

    private List<ZValueRange> fillUnspecifiedRangesWithDefaults(List<ZValueRange> valueRanges)
    {
        List<ZValueRange> nonNullValues = new ArrayList<>(valueRanges.size());
        for (int index = 0; index < valueRanges.size(); index++) {
            ZValueRange range = valueRanges.get(index);

            Integer minimumValue = range.getMinimumValue();
            Integer maximumValue = range.getMaximumValue();

            if (minimumValue == null) {
                minimumValue = -(1 << encodingBits.get(index));
            }
            if (maximumValue == null) {
                maximumValue = (1 << encodingBits.get(index)) - 1;
            }

            nonNullValues.add(new ZValueRange(Optional.of(minimumValue), Optional.of(maximumValue)));
        }
        return nonNullValues;
    }

    /**
     * Recursive function to find all address ranges within given value ranges.
     * <p/>
     * Returns when the address range is either completely within or completely out of the value ranges.
     * Recurses to a smaller Z-order curve if the address range only partially overlaps with the value ranges.
     *
     * @param level the level of recursion on the Z-curve
     * @param startAddress the starting address of the current Z-curve level
     * @param totalCurveBits the amount of bits in the current Z-curve level
     */
    private void findAddressesRecursively(int level, long startAddress, int totalCurveBits, List<ZValueRange> valueRanges, List<ZAddressRange<Long>> addressRanges)
    {
        long endAddress = startAddress + ((long) 1 << totalCurveBits) - 1;

        List<Integer> startValues = decode(startAddress);
        List<Integer> endValues = decode(endAddress);

        if (inRange(startValues, endValues, valueRanges)) {
            addressRanges.add(new ZAddressRange<>(startAddress, endAddress));
            return;
        }

        if (outOfRange(startValues, endValues, valueRanges)) {
            return;
        }

        totalCurveBits -= dimensions[level];
        int numOfSubspaces = 1 << dimensions[level];
        for (int i = 0; i < numOfSubspaces; i++) {
            findAddressesRecursively(level - 1, startAddress, totalCurveBits, valueRanges, addressRanges);
            startAddress += ((long) 1 << totalCurveBits);
        }
    }

    private boolean inRange(List<Integer> startValues, List<Integer> endValues, List<ZValueRange> valueRanges)
    {
        for (int i = 0; i < valueRanges.size(); i++) {
            if ((valueRanges.get(i).getMinimumValue() > startValues.get(i)) || (valueRanges.get(i).getMaximumValue() < endValues.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean outOfRange(List<Integer> startValues, List<Integer> endValues, List<ZValueRange> valueRanges)
    {
        for (int i = 0; i < valueRanges.size(); i++) {
            if ((valueRanges.get(i).getMinimumValue() > endValues.get(i)) || (valueRanges.get(i).getMaximumValue() < startValues.get(i))) {
                return true;
            }
        }
        return false;
    }

    private static List<ZAddressRange<Long>> combineAddressRanges(List<ZAddressRange<Long>> addressRanges)
    {
        if (addressRanges.isEmpty()) {
            return addressRanges;
        }

        addressRanges.sort((range1, range2) -> {
            if (range1.equals(range2)) {
                return 0;
            }
            if (range1.getMinimumAddress().equals(range2.getMinimumAddress())) {
                return (range1.getMaximumAddress() > range2.getMaximumAddress()) ? 1 : -1;
            }
            return (range1.getMinimumAddress() > range2.getMinimumAddress()) ? 1 : -1;
        });

        List<ZAddressRange<Long>> combinedAddressRanges = new ArrayList<>();
        combinedAddressRanges.add(addressRanges.get(0));

        for (int index = 1; index < addressRanges.size(); index++) {
            ZAddressRange<Long> previousAddressRange = combinedAddressRanges.get(combinedAddressRanges.size() - 1);
            ZAddressRange<Long> currentAddressRange = addressRanges.get(index);

            if (isOverlapping(previousAddressRange, currentAddressRange)) {
                combinedAddressRanges.set(combinedAddressRanges.size() - 1, new ZAddressRange<>(previousAddressRange.getMinimumAddress(), currentAddressRange.getMaximumAddress()));
            }
            else {
                combinedAddressRanges.add(currentAddressRange);
            }
        }

        return combinedAddressRanges;
    }

    private static boolean isOverlapping(ZAddressRange<Long> previousAddressRange, ZAddressRange<Long> currentAddressRange)
    {
        return previousAddressRange.getMaximumAddress() + 1 >= currentAddressRange.getMinimumAddress();
    }
}
