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
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * The ZOrderCurveSearcher class contains functions to search for addresses within given value ranges.
 */
public class ZOrderCurveSearcher
{
    private enum RangeType {
        INTEGER, LONG
    }

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
     * Public class "constructor" specifying ZOrder class variables and initializing <code>dimensions</code>.
     * <p/>
     * The <code>dimensions</code> array stores how many values are stored at each level of the Z-curve.
     */
    public static ZOrderCurveSearcher createZOrderCurveSearcher(ZOrder zOrder, List<Integer> encodingBits, int totalBitLength, int maxBitLength)
    {
        return new ZOrderCurveSearcher(zOrder, encodingBits, totalBitLength, maxBitLength, initializeDimensions(encodingBits, maxBitLength));
    }

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
     * Searches for and outputs ranges of long addresses within certain ranges in each dimension.
     */
    public List<ZAddressRange<Long>> zOrderSearchCurveLongs(List<ZValueRange> ranges)
    {
        return zOrderSearchCurve(ranges, RangeType.LONG);
    }

    /**
     * Searches for and outputs ranges of integer addresses within certain ranges in each dimension.
     */
    public List<ZAddressRange<Integer>> zOrderSearchCurveIntegers(List<ZValueRange> ranges)
    {
        return zOrderSearchCurve(ranges, RangeType.INTEGER).stream()
                .map(addressRange -> new ZAddressRange<>(addressRange.getMinimumAddress().intValue(), addressRange.getMaximumAddress().intValue())).collect(Collectors.toList());
    }

    /**
     * Searches for and outputs ranges of addresses within certain ranges in each dimension.
     * <p/>
     * Every two elements in the output list represent the minimum and maximum of an address range.
     * <p/>
     * For example, if the list is {1, 5, 10, 14}, the address ranges are from {1, 5} and {10, 14}.
     *
     * @param valueRanges the list of minimum and maximum values of each dimension
     * @return the list of address ranges within the given value ranges
     */
    private List<ZAddressRange<Long>> zOrderSearchCurve(List<ZValueRange> valueRanges, RangeType rangeType)
    {
        List<ZAddressRange<Long>> addressRanges = new ArrayList<>();
        findAddressesRecursively(maxBitLength - 1, 0L, totalBitLength, addressRanges, fillInNulls(valueRanges), rangeType);
        return addressRanges;
    }

    private List<ZValueRange> fillInNulls(List<ZValueRange> valueRanges)
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
    private void findAddressesRecursively(int level, long startAddress, int totalCurveBits, List<ZAddressRange<Long>> addressRanges, List<ZValueRange> valueRanges, RangeType rangeType)
    {
        long endAddress = startAddress + ((long) 1 << totalCurveBits) - 1;

        List<Integer> startValues = zOrder.decode(startAddress);
        List<Integer> endValues = zOrder.decode(endAddress);

        if (inRange(startValues, endValues, valueRanges)) {
            addToAddressList(startAddress, endAddress, addressRanges, rangeType);
            return;
        }

        if (outOfRange(startValues, endValues, valueRanges)) {
            return;
        }

        totalCurveBits -= dimensions[level];
        int subtrees = 1 << dimensions[level];
        for (int i = 0; i < subtrees; i++) {
            findAddressesRecursively(level - 1, startAddress, totalCurveBits, addressRanges, valueRanges, rangeType);
            startAddress += ((long) 1 << totalCurveBits);
        }
    }

    private void addToAddressList(long startAddress, long endAddress, List<ZAddressRange<Long>> addressRanges, RangeType rangeType)
    {
        if (rangeType == RangeType.INTEGER) {
            checkArgument((startAddress <= Integer.MAX_VALUE) && (endAddress <= Integer.MAX_VALUE),
                    format("The address range [%d, %d] contains addresses greater than integers.", startAddress, endAddress));
        }

        int lastIndex = addressRanges.size() - 1;

        if ((addressRanges.size() > 0) && (startAddress == addressRanges.get(lastIndex).getMaximumAddress() + 1)) {
            ZAddressRange<Long> addressRange = addressRanges.get(lastIndex);
            addressRanges.set(lastIndex, new ZAddressRange<>(addressRange.getMinimumAddress(), endAddress));
        }
        else {
            addressRanges.add(new ZAddressRange<>(startAddress, endAddress));
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
}
