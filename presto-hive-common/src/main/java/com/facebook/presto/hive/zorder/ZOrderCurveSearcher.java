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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The ZOrderCurveSearcher class contains functions to search for addresses within given value ranges.
 */
public class ZOrderCurveSearcher
{
    private final ZOrder zOrder;

    private final List<Integer> encodingBits;
    private final int totalBitLength;
    private final int maxBitLength;

    private final int[] dimensions;

    private ZOrderCurveSearcher(ZOrder zOrder, List<Integer> encodingBits, int totalBitLength, int maxBitLength, int[] dimensions)
    {
        this.zOrder = zOrder;

        this.encodingBits = encodingBits;
        this.totalBitLength = totalBitLength;
        this.maxBitLength = maxBitLength;

        this.dimensions = dimensions;
    }

    /**
     * Public class "constructor" specifying ZOrder class variables
     */
    public static ZOrderCurveSearcher createZOrderCurveSearcher(ZOrder zOrder, List<Integer> encodingBits, int totalBitLength, int maxBitLength)
    {
        return new ZOrderCurveSearcher(zOrder, encodingBits, totalBitLength, maxBitLength, initializeDimensions(encodingBits, maxBitLength));
    }

    /**
     * Initializes <code>dimensions</code> array, which stores how many values are stored at each level of the Z-curve.
     * <p/>
     * For example, given <code>encodingBits</code> = [2, 8, 6] and <code>maxBitLength</code> = 6, <code>dimensions</code> = [3, 3, 2, 2, 2, 2, 1, 1].
     */
    private static int[] initializeDimensions(List<Integer> encodingBits, int maxBitLength)
    {
        int[] dimensions = new int[maxBitLength];
        List<Integer> bitLengths = new ArrayList<>(encodingBits);
        for (int dimensionIndex = maxBitLength - 1; dimensionIndex >= 0; dimensionIndex--) {
            for (int bitLengthIndex = 0; bitLengthIndex < bitLengths.size(); bitLengthIndex++) {
                if (bitLengths.get(bitLengthIndex) > 0) {
                    dimensions[dimensionIndex]++;
                    bitLengths.set(bitLengthIndex, bitLengths.get(bitLengthIndex) - 1);
                }
            }
        }
        return dimensions;
    }

    /**
     * Searches for ranges of addresses within given value ranges in each dimension.
     * <p/>
     * Every two elements in the output list represent the minimum and maximum of an address range.
     * <p/>
     * For example, if the list is {1, 5, 10, 14}, the address ranges are from {1, 5} and {10, 14}.
     *
     * @param valueRanges the list of minimum and maximum values of each dimension
     */
    List<ZAddressRange<Long>> zOrderSearchCurve(List<ZValueRange> valueRanges)
    {
        List<ZAddressRange<Long>> addressRanges = new ArrayList<>();

        findAddressesRecursively(maxBitLength - 1, 0L, totalBitLength, addressRanges, fillUnspecifiedRangesWithDefaults(valueRanges));

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
                minimumValue = 0;
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
    private void findAddressesRecursively(int level, long startAddress, int totalCurveBits, List<ZAddressRange<Long>> addressRanges, List<ZValueRange> valueRanges)
    {
        long endAddress = startAddress + ((long) 1 << totalCurveBits) - 1;

        List<Integer> startValues = zOrder.decode(startAddress);
        List<Integer> endValues = zOrder.decode(endAddress);

        if (inRange(startValues, endValues, valueRanges)) {
            addressRanges.add(new ZAddressRange<>(startAddress, endAddress));
            return;
        }

        if (outOfRange(startValues, endValues, valueRanges)) {
            return;
        }

        totalCurveBits -= dimensions[level];
        int subtrees = 1 << dimensions[level];
        for (int i = 0; i < subtrees; i++) {
            findAddressesRecursively(level - 1, startAddress, totalCurveBits, addressRanges, valueRanges);
            startAddress += ((long) 1 << totalCurveBits);
        }
    }

    private boolean inRange(List<Integer> startValues, List<Integer> endValues, List<ZValueRange> valueRanges)
    {
        for (int i = 0; i < encodingBits.size(); i++) {
            if ((valueRanges.get(i).getMinimumValue() > startValues.get(i)) || (valueRanges.get(i).getMaximumValue() < endValues.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean outOfRange(List<Integer> startValues, List<Integer> endValues, List<ZValueRange> valueRanges)
    {
        for (int i = 0; i < encodingBits.size(); i++) {
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
        return previousAddressRange.getMaximumAddress() > currentAddressRange.getMinimumAddress();
    }
}
