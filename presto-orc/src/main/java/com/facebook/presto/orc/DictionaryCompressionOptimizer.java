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
package com.facebook.presto.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class DictionaryCompressionOptimizer
{
    private static final double DICTIONARY_MIN_COMPRESSION_RATIO = 1.25;
    private static final double DICTIONARY_ALWAYS_KEEP_COMPRESSION_RATIO = 3.0;

    // Instead of waiting for the dictionary to fill completely, which would force a column into
    // direct mode, close the stripe early assuming it has hit the minimum row count.
    static final DataSize DICTIONARY_MEMORY_MAX_RANGE = new DataSize(4, Unit.MEGABYTE);

    private final Set<DictionaryColumnManager> allWriters;
    private final Set<DictionaryColumnManager> dictionaryWriters = new HashSet<>();

    private final int stripeMinBytes;
    private final int stripeMaxBytes;
    private final int stripeMaxRowCount;
    private final int dictionaryMemoryMaxBytesLow;
    private final int dictionaryMemoryMaxBytesHigh;

    private int dictionaryMemoryBytes;

    public DictionaryCompressionOptimizer(
            Set<? extends DictionaryColumn> writers,
            int stripeMinBytes,
            int stripeMaxBytes,
            int stripeMaxRowCount,
            int dictionaryMemoryMaxBytes)
    {
        requireNonNull(writers, "writers is null");
        this.allWriters = ImmutableSet.copyOf(writers.stream()
                .map(DictionaryColumnManager::new)
                .collect(toSet()));

        checkArgument(stripeMinBytes >= 0, "stripeMinBytes is negative");
        this.stripeMinBytes = stripeMinBytes;

        checkArgument(stripeMaxBytes >= stripeMinBytes, "stripeMaxBytes is less than stripeMinBytes");
        this.stripeMaxBytes = stripeMaxBytes;

        checkArgument(stripeMaxRowCount >= 0, "stripeMaxRowCount is negative");
        this.stripeMaxRowCount = stripeMaxRowCount;

        checkArgument(dictionaryMemoryMaxBytes >= 0, "dictionaryMemoryMaxBytes is negative");
        this.dictionaryMemoryMaxBytesHigh = dictionaryMemoryMaxBytes;
        this.dictionaryMemoryMaxBytesLow = (int) Math.max(dictionaryMemoryMaxBytes - DICTIONARY_MEMORY_MAX_RANGE.toBytes(), 0);

        dictionaryWriters.addAll(allWriters);
    }

    public int getDictionaryMemoryBytes()
    {
        return dictionaryMemoryBytes;
    }

    public boolean isFull(long bufferedBytes)
    {
        // if the strip is big enough to flush, stop before we hit the absolute max, so we are
        // not forced to convert a dictionary to direct to fit in memory
        if (bufferedBytes > stripeMinBytes) {
            return dictionaryMemoryBytes > dictionaryMemoryMaxBytesLow;
        }
        // strip is small, grow to the high water mark (so at the very least we have more information)
        return dictionaryMemoryBytes > dictionaryMemoryMaxBytesHigh;
    }

    public void reset()
    {
        dictionaryWriters.clear();
        dictionaryWriters.addAll(allWriters);
        dictionaryMemoryBytes = 0;
        allWriters.forEach(DictionaryColumnManager::reset);
    }

    public void finalOptimize()
    {
        convertLowCompressionStreams();
    }

    public void optimize(int bufferedBytes, int stripeRowCount)
    {
        // recompute the dictionary memory usage
        dictionaryMemoryBytes = dictionaryWriters.stream()
                .mapToInt(DictionaryColumnManager::getDictionaryBytes)
                .sum();

        // update the dictionary growth history
        dictionaryWriters.forEach(column -> column.updateHistory(stripeRowCount));

        if (dictionaryMemoryBytes <= dictionaryMemoryMaxBytesLow) {
            return;
        }

        // before any further checks, convert all low compression streams
        convertLowCompressionStreams();

        if (dictionaryMemoryBytes <= dictionaryMemoryMaxBytesLow) {
            return;
        }

        // calculate size of non-dictionary columns by removing the buffered size of dictionary columns
        int nonDictionaryBufferedBytes = bufferedBytes;
        for (DictionaryColumnManager dictionaryWriter : dictionaryWriters) {
            nonDictionaryBufferedBytes -= dictionaryWriter.getBufferedBytes();
        }

        // convert dictionary columns to direct until we are blow the high memory limit
        while (dictionaryMemoryBytes > dictionaryMemoryMaxBytesHigh) {
            DictionaryCompressionProjection projection = selectDictionaryColumnToConvert(nonDictionaryBufferedBytes, stripeRowCount);
            if (projection.getColumnToConvert().getCompressionRatio() >= DICTIONARY_ALWAYS_KEEP_COMPRESSION_RATIO) {
                return;
            }
            nonDictionaryBufferedBytes += convertToDirect(projection.getColumnToConvert());
        }

        // if the stripe is larger then the minimum row count, we are not required to convert any more dictionary columns to direct
        if (nonDictionaryBufferedBytes + dictionaryMemoryBytes >= stripeMinBytes) {
            // check if we can get better compression by converting a dictionary column to direct.  This can happen when then there are multiple
            // dictionary columns and one does not compress well, so if we convert it to direct we can continue to use the existing dictionaries
            // for the other columns.
            double currentCompressionRatio = currentCompressionRatio(nonDictionaryBufferedBytes);
            while (!dictionaryWriters.isEmpty()) {
                DictionaryCompressionProjection projection = selectDictionaryColumnToConvert(nonDictionaryBufferedBytes, stripeRowCount);
                if (projection.getPredictedFileCompressionRatio() < currentCompressionRatio) {
                    return;
                }
                nonDictionaryBufferedBytes += convertToDirect(projection.getColumnToConvert());
            }
        }
    }

    private void convertLowCompressionStreams()
    {
        // convert all low compression column to direct
        ImmutableList.copyOf(dictionaryWriters).stream()
                .filter(dictionaryWriter -> dictionaryWriter.getCompressionRatio() < DICTIONARY_MIN_COMPRESSION_RATIO)
                .forEach(this::convertToDirect);
    }

    private long convertToDirect(DictionaryColumnManager dictionaryWriter)
    {
        dictionaryMemoryBytes -= dictionaryWriter.getDictionaryBytes();
        long directBytes = dictionaryWriter.convertToDirect();
        dictionaryWriters.remove(dictionaryWriter);
        return directBytes;
    }

    private double currentCompressionRatio(int allOtherBytes)
    {
        long uncompressedBytes = allOtherBytes;
        long compressedBytes = allOtherBytes;

        for (DictionaryColumnManager column : dictionaryWriters) {
            uncompressedBytes += column.getRawBytes();
            compressedBytes += column.getDictionaryBytes();
        }
        return 1.0 * uncompressedBytes / compressedBytes;
    }

    /**
     * Choose a dictionary column to convert to direct encoding.  We do this by predicting the compression ration
     * of the stripe if a singe column is flipped to direct.  So for each column, we try to predict the row count
     * when we will hit a stripe flush limit if that column were converted to direct.  Once we know the row count, we
     * calculate the predicted compression ratio.
     *
     * @param totalNonDictionaryBytes current size of the stripe without non-dictionary columns
     * @param stripeRowCount current number of rows in the stripe
     * @return the column that would produce the best stripe compression ration if converted to direct
     */
    private DictionaryCompressionProjection selectDictionaryColumnToConvert(int totalNonDictionaryBytes, int stripeRowCount)
    {
        checkState(!dictionaryWriters.isEmpty());

        int totalNonDictionaryBytesPerRow = totalNonDictionaryBytes / stripeRowCount;

        // rawBytes = sum of the length of every row value (without dictionary encoding)
        // dictionaryBytes = sum of the length of every entry in the dictionary
        // indexBytes = bytes used encode the dictionary index (e.g., 2 byte for dictionary less than 65536 entries)

        long totalDictionaryRawBytes = 0;
        long totalDictionaryBytes = 0;
        long totalDictionaryIndexBytes = 0;

        long totalDictionaryRawBytesPerRow = 0;
        long totalDictionaryBytesPerNewRow = 0;
        long totalDictionaryIndexBytesPerRow = 0;

        for (DictionaryColumnManager column : dictionaryWriters) {
            totalDictionaryRawBytes += column.getRawBytes();
            totalDictionaryBytes += column.getDictionaryBytes();
            totalDictionaryIndexBytes += column.getIndexBytes();

            totalDictionaryRawBytesPerRow += column.getRawBytesPerRow();
            totalDictionaryBytesPerNewRow += column.getDictionaryBytesPerFutureRow();
            totalDictionaryIndexBytesPerRow += column.getIndexBytesPerRow();
        }

        long totalUncompressedBytesPerRow = totalNonDictionaryBytesPerRow + totalDictionaryRawBytesPerRow;

        DictionaryCompressionProjection maxProjectedCompression = null;
        for (DictionaryColumnManager column : dictionaryWriters) {
            // determine the size of the currently written stripe if we were convert this column to direct
            long currentRawBytes = totalNonDictionaryBytes + column.getRawBytes();
            long currentDictionaryBytes = totalDictionaryBytes - column.getDictionaryBytes();
            long currentIndexBytes = totalDictionaryIndexBytes - column.getIndexBytes();
            long currentTotalBytes = currentRawBytes + currentDictionaryBytes + currentIndexBytes;

            // estimate the size of each new row if we were convert this column to direct
            double rawBytesPerFutureRow = totalNonDictionaryBytesPerRow + column.getRawBytesPerRow();
            double dictionaryBytesPerFutureRow = totalDictionaryBytesPerNewRow - column.getDictionaryBytesPerFutureRow();
            double indexBytesPerFutureRow = totalDictionaryIndexBytesPerRow - column.getIndexBytesPerRow();
            double totalBytesPerFutureRow = rawBytesPerFutureRow + dictionaryBytesPerFutureRow + indexBytesPerFutureRow;

            // estimate how many rows until we hit a limit and flush the stripe if we convert this column to direct
            long rowsToDictionaryMemoryLimit = (long) ((dictionaryMemoryMaxBytesLow - currentDictionaryBytes) / dictionaryBytesPerFutureRow);
            long rowsToStripeMemoryLimit = (long) ((stripeMaxBytes - currentTotalBytes) / totalBytesPerFutureRow);
            long rowsToStripeRowLimit = stripeMaxRowCount - stripeRowCount;
            long rowsToLimit = Longs.min(rowsToDictionaryMemoryLimit, rowsToStripeMemoryLimit, rowsToStripeRowLimit);

            // predict the compression ratio at that limit if we were convert this column to direct
            long predictedUncompressedSizeAtLimit = totalNonDictionaryBytes + totalDictionaryRawBytes + (totalUncompressedBytesPerRow * rowsToLimit);
            long predictedCompressedSizeAtLimit = (long) (currentTotalBytes + (totalBytesPerFutureRow * rowsToLimit));
            double predictedCompressionRatioAtLimit = 1.0 * predictedUncompressedSizeAtLimit / predictedCompressedSizeAtLimit;

            // convert the column that creates the best compression ratio
            if (maxProjectedCompression == null || maxProjectedCompression.getPredictedFileCompressionRatio() < predictedCompressionRatioAtLimit) {
                maxProjectedCompression = new DictionaryCompressionProjection(column, predictedCompressionRatioAtLimit);
            }
        }
        return maxProjectedCompression;
    }

    public static int estimateIndexBytesPerValue(int dictionaryEntries)
    {
        // assume basic byte packing
        if (dictionaryEntries <= 256) {
            return 1;
        }
        if (dictionaryEntries <= 65_536) {
            return 2;
        }
        if (dictionaryEntries <= 16_777_216) {
            return 3;
        }
        return 4;
    }

    public interface DictionaryColumn
    {
        int getValueCount();

        int getNonNullValueCount();

        long getRawBytes();

        int getDictionaryEntries();

        int getDictionaryBytes();

        long convertToDirect();
    }

    private static class DictionaryColumnManager
    {
        private final DictionaryColumn dictionaryColumn;
        private boolean directEncoded;

        private int rowCount;

        private int pastValueCount;
        private int pastDictionaryEntries;

        private int pendingPastValueCount;
        private int pendingPastDictionaryEntries;

        public DictionaryColumnManager(DictionaryColumn dictionaryColumn)
        {
            this.dictionaryColumn = dictionaryColumn;
        }

        long convertToDirect()
        {
            directEncoded = true;
            return dictionaryColumn.convertToDirect();
        }

        void reset()
        {
            directEncoded = false;

            pastValueCount = 0;
            pastDictionaryEntries = 0;

            pendingPastValueCount = 0;
            pendingPastDictionaryEntries = 0;
        }

        public void updateHistory(int rowCount)
        {
            this.rowCount = rowCount;
            int currentValueCount = dictionaryColumn.getValueCount();
            if (currentValueCount - pendingPastValueCount >= 1024) {
                pastValueCount = pendingPastValueCount;
                pastDictionaryEntries = pendingPastDictionaryEntries;

                pendingPastValueCount = currentValueCount;
                pendingPastDictionaryEntries = dictionaryColumn.getDictionaryEntries();
            }
        }

        public long getRawBytes()
        {
            checkState(!directEncoded);
            return dictionaryColumn.getRawBytes();
        }

        public double getRawBytesPerRow()
        {
            checkState(!directEncoded);
            return 1.0 * getRawBytes() / rowCount;
        }

        public int getDictionaryBytes()
        {
            checkState(!directEncoded);
            return dictionaryColumn.getDictionaryBytes();
        }

        public double getDictionaryBytesPerFutureRow()
        {
            checkState(!directEncoded);

            int currentDictionaryEntries = dictionaryColumn.getDictionaryEntries();
            int currentValueCount = dictionaryColumn.getValueCount();

            // average size of a dictionary entry
            double dictionaryBytesPerEntry = 1.0 * dictionaryColumn.getDictionaryBytes() / currentDictionaryEntries;

            // average number of entries added per non-value over the "past" period
            double dictionaryEntriesPerFutureValue = 1.0 * (currentDictionaryEntries - pastDictionaryEntries) / (currentValueCount - pastValueCount);

            // Expected size comes down to: chance for a non-null row * chance for a unique dictionary value * average bytes per dictionary entry
            return dictionaryBytesPerEntry * dictionaryEntriesPerFutureValue;
        }

        public int getIndexBytes()
        {
            checkState(!directEncoded);
            return estimateIndexBytesPerValue(dictionaryColumn.getDictionaryEntries()) * dictionaryColumn.getNonNullValueCount();
        }

        public double getIndexBytesPerRow()
        {
            checkState(!directEncoded);
            return 1.0 * getIndexBytes() / rowCount;
        }

        public int getNullsBytes()
        {
            checkState(!directEncoded);
            return (dictionaryColumn.getValueCount() - dictionaryColumn.getNonNullValueCount() + 7) / 8;
        }

        public int getCompressedBytes()
        {
            return getDictionaryBytes() + getIndexBytes() + getNullsBytes();
        }

        public double getCompressionRatio()
        {
            checkState(!directEncoded);
            return 1.0 * getRawBytes() / getCompressedBytes();
        }

        public long getBufferedBytes()
        {
            return getIndexBytes() + getDictionaryBytes();
        }
    }

    private static class DictionaryCompressionProjection
    {
        private final DictionaryColumnManager columnToConvert;
        private final double predictedFileCompressionRatio;

        public DictionaryCompressionProjection(DictionaryColumnManager columnToConvert, double predictedFileCompressionRatio)
        {
            this.columnToConvert = requireNonNull(columnToConvert, "columnToConvert is null");
            this.predictedFileCompressionRatio = predictedFileCompressionRatio;
        }

        public DictionaryColumnManager getColumnToConvert()
        {
            return columnToConvert;
        }

        public double getPredictedFileCompressionRatio()
        {
            return predictedFileCompressionRatio;
        }
    }
}
