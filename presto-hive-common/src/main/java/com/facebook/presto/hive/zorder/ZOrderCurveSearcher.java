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
import java.util.Arrays;
import java.util.List;
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

    /**
     * The BoundedZValueRange class contains non-null Integer minimum and maximum value lists. The bounds are all inclusive.
     */
    private static class BoundedZValueRange
    {
        private final List<Integer> minimumValues;
        private final List<Integer> maximumValues;

        public BoundedZValueRange(List<Integer> minimumValues, List<Integer> maximumValues)
        {
            this.minimumValues = minimumValues;
            this.maximumValues = maximumValues;
        }

        public Integer getMinimumValueAt(int index)
        {
            return minimumValues.get(index);
        }

        public Integer getMaximumValueAt(int index)
        {
            return maximumValues.get(index);
        }
    }

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
        List<BoundedZValueRange> boundedValueRanges = fillInNulls(valueRanges);

        int[] lengths = new int[valueRanges.size()];
        for (int index = 0; index < valueRanges.size(); index++) {
            lengths[index] = valueRanges.get(index).length;
        }

        int[] counters = new int[valueRanges.size()];
        Arrays.fill(counters, 0);

        List<ZAddressRange<Long>> addressRanges = new ArrayList<>();
        permuteValueRanges(counters, lengths, 0, addressRanges, boundedValueRanges, rangeType);

        addressRanges = combineAddressRanges(addressRanges);
        return addressRanges;
    }

    private List<BoundedZValueRange> fillInNulls(List<ZValueRange> valueRanges)
    {
        List<BoundedZValueRange> nonNullValues = new ArrayList<>(valueRanges.size());
        for (int index = 0; index < valueRanges.size(); index++) {
            ZValueRange range = valueRanges.get(index);

            List<Integer> minimumValues = new ArrayList<>();
            for (Integer minimumValue : range.getMinimumValues()) {
                if (minimumValue == null) {
                    minimumValue = 0;
                }
                minimumValues.add(minimumValue);
            }

            List<Integer> maximumValues = new ArrayList<>();
            for (Integer maximumValue : range.getMaximumValues()) {
                if (maximumValue == null) {
                    maximumValue = (1 << encodingBits.get(index)) - 1;
                }
                maximumValues.add(maximumValue);
            }

            nonNullValues.add(new BoundedZValueRange(minimumValues, maximumValues));
        }
        return nonNullValues;
    }

    private void permuteValueRanges(int[] counters, int[] lengths, int level, List<ZAddressRange<Long>> addressRanges, List<BoundedZValueRange> valueRanges, RangeType rangeType)
    {
        if (level == counters.length) {
            findAddressesRecursively(maxBitLength - 1, 0L, totalBitLength, addressRanges, valueRanges, rangeType, counters);
        }
        else {
            for (counters[level] = 0; counters[level] < lengths[level]; counters[level]++) {
                permuteValueRanges(counters, lengths, level + 1, addressRanges, valueRanges, rangeType);
            }
        }
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
    private void findAddressesRecursively(
            int level, long startAddress, int totalCurveBits,
            List<ZAddressRange<Long>> addressRanges, List<BoundedZValueRange> valueRanges, RangeType rangeType, int[] zRangeIndexes)
    {
        long endAddress = startAddress + ((long) 1 << totalCurveBits) - 1;

        List<Integer> startValues = zOrder.decode(startAddress);
        List<Integer> endValues = zOrder.decode(endAddress);

        if (inRange(startValues, endValues, valueRanges, zRangeIndexes)) {
            addToAddressList(startAddress, endAddress, addressRanges, rangeType);
            return;
        }

        if (outOfRange(startValues, endValues, valueRanges, zRangeIndexes)) {
            return;
        }

        checkArgument(level >= 0 && level < maxBitLength, format("Level %d out of expected range [0 - %d]", level, maxBitLength));

        totalCurveBits -= dimensions[level];
        int subtrees = 1 << dimensions[level];
        for (int i = 0; i < subtrees; i++) {
            findAddressesRecursively(level - 1, startAddress, totalCurveBits, addressRanges, valueRanges, rangeType, zRangeIndexes);
            startAddress += ((long) 1 << totalCurveBits);
        }
    }

    private void addToAddressList(long startAddress, long endAddress, List<ZAddressRange<Long>> addressRanges, RangeType rangeType)
    {
        if (rangeType == RangeType.INTEGER) {
            checkArgument((startAddress <= Integer.MAX_VALUE) && (endAddress <= Integer.MAX_VALUE),
                    format("The address range [%d, %d] contains addresses greater than integers.", startAddress, endAddress));
        }

        addressRanges.add(new ZAddressRange<>(startAddress, endAddress));
    }

    private boolean inRange(List<Integer> startValues, List<Integer> endValues, List<BoundedZValueRange> valueRanges, int[] zRangeIndexes)
    {
        for (int i = 0; i < encodingBits.size(); i++) {
            if ((valueRanges.get(i).getMinimumValueAt(zRangeIndexes[i]) > startValues.get(i)) || (valueRanges.get(i).getMaximumValueAt(zRangeIndexes[i]) < endValues.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean outOfRange(List<Integer> startValues, List<Integer> endValues, List<BoundedZValueRange> valueRanges, int[] zRangeIndexes)
    {
        for (int i = 0; i < encodingBits.size(); i++) {
            if ((valueRanges.get(i).getMinimumValueAt(zRangeIndexes[i]) > endValues.get(i)) || (valueRanges.get(i).getMaximumValueAt(zRangeIndexes[i]) < startValues.get(i))) {
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

        List<ZAddressRange<Long>> sortedAddressRanges = new ArrayList<>(addressRanges);
        sortedAddressRanges.sort((range1, range2) -> {
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
        return (previousAddressRange.getMinimumAddress().equals(currentAddressRange.getMinimumAddress()))
                || (previousAddressRange.getMaximumAddress().equals(currentAddressRange.getMaximumAddress()))
                || (previousAddressRange.getMaximumAddress() + 1 == currentAddressRange.getMinimumAddress())
                || (previousAddressRange.getMaximumAddress() > currentAddressRange.getMinimumAddress());
    }
}
