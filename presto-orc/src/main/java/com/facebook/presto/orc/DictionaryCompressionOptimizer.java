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
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class DictionaryCompressionOptimizer
{
    private static final double DICTIONARY_MIN_COMPRESSION_RATIO = 1.25;

    // Instead of waiting for the dictionary to fill completely, which would force a column into
    // direct mode, close the stripe early assuming it has hit the minimum row count.
    static final DataSize DICTIONARY_MEMORY_MAX_RANGE = new DataSize(4, Unit.MEGABYTE);

    static final DataSize DIRECT_COLUMN_SIZE_RANGE = new DataSize(4, Unit.MEGABYTE);

    private final Set<DictionaryColumnManager> allWriters;
    private final Set<DictionaryColumnManager> directConversionCandidates = new HashSet<>();

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

        directConversionCandidates.addAll(allWriters);
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
        directConversionCandidates.clear();
        directConversionCandidates.addAll(allWriters);
        dictionaryMemoryBytes = 0;
        allWriters.forEach(DictionaryColumnManager::reset);
    }

    public void finalOptimize(int bufferedBytes)
    {
        convertLowCompressionStreams(bufferedBytes);
    }

    public void optimize(int bufferedBytes, int stripeRowCount)
    {
        // recompute the dictionary memory usage
        dictionaryMemoryBytes = allWriters.stream()
                .filter(writer -> !writer.isDirectEncoded())
                .mapToInt(DictionaryColumnManager::getDictionaryBytes)
                .sum();

        // update the dictionary growth history
        allWriters.stream()
                .filter(writer -> !writer.isDirectEncoded())
                .forEach(column -> column.updateHistory(stripeRowCount));

        if (dictionaryMemoryBytes <= dictionaryMemoryMaxBytesLow) {
            return;
        }

        // before any further checks, convert all low compression streams
        bufferedBytes = convertLowCompressionStreams(bufferedBytes);

        if (dictionaryMemoryBytes <= dictionaryMemoryMaxBytesLow || bufferedBytes >= stripeMaxBytes) {
            return;
        }

        // calculate size of non-dictionary columns by removing the buffered size of dictionary columns
        int nonDictionaryBufferedBytes = bufferedBytes;
        for (DictionaryColumnManager dictionaryWriter : allWriters) {
            if (!dictionaryWriter.isDirectEncoded()) {
                nonDictionaryBufferedBytes -= dictionaryWriter.getBufferedBytes();
            }
        }

        // convert dictionary columns to direct until we are below the high memory limit
        while (!directConversionCandidates.isEmpty() && dictionaryMemoryBytes > dictionaryMemoryMaxBytesHigh && bufferedBytes < stripeMaxBytes) {
            DictionaryCompressionProjection projection = selectDictionaryColumnToConvert(nonDictionaryBufferedBytes, stripeRowCount);
            int selectDictionaryColumnBufferedBytes = toIntExact(projection.getColumnToConvert().getBufferedBytes());

            OptionalInt directBytes = tryConvertToDirect(projection.getColumnToConvert(), getMaxDirectBytes(bufferedBytes));
            if (directBytes.isPresent()) {
                bufferedBytes = bufferedBytes + directBytes.getAsInt() - selectDictionaryColumnBufferedBytes;
                nonDictionaryBufferedBytes += directBytes.getAsInt();
            }
        }

        if (bufferedBytes >= stripeMaxBytes) {
            return;
        }

        // if the stripe is larger then the minimum stripe size, we are not required to convert any more dictionary columns to direct
        if (bufferedBytes >= stripeMinBytes) {
            // check if we can get better compression by converting a dictionary column to direct.  This can happen when then there are multiple
            // dictionary columns and one does not compress well, so if we convert it to direct we can continue to use the existing dictionaries
            // for the other columns.
            double currentCompressionRatio = currentCompressionRatio(nonDictionaryBufferedBytes);
            while (!directConversionCandidates.isEmpty() && bufferedBytes < stripeMaxBytes) {
                DictionaryCompressionProjection projection = selectDictionaryColumnToConvert(nonDictionaryBufferedBytes, stripeRowCount);
                if (projection.getPredictedFileCompressionRatio() < currentCompressionRatio) {
                    return;
                }

                int selectDictionaryColumnBufferedBytes = toIntExact(projection.getColumnToConvert().getBufferedBytes());
                OptionalInt directBytes = tryConvertToDirect(projection.getColumnToConvert(), getMaxDirectBytes(bufferedBytes));
                if (directBytes.isPresent()) {
                    bufferedBytes = bufferedBytes + directBytes.getAsInt() - selectDictionaryColumnBufferedBytes;
                    nonDictionaryBufferedBytes += directBytes.getAsInt();
                }
            }
        }
    }

    private int convertLowCompressionStreams(int bufferedBytes)
    {
        // convert all low compression column to direct
        for (DictionaryColumnManager dictionaryWriter : ImmutableList.copyOf(directConversionCandidates)) {
            if (dictionaryWriter.getCompressionRatio() < DICTIONARY_MIN_COMPRESSION_RATIO) {
                int columnBufferedBytes = toIntExact(dictionaryWriter.getBufferedBytes());
                OptionalInt directBytes = tryConvertToDirect(dictionaryWriter, getMaxDirectBytes(bufferedBytes));
                if (directBytes.isPresent()) {
                    bufferedBytes = bufferedBytes + directBytes.getAsInt() - columnBufferedBytes;
                    if (bufferedBytes >= stripeMaxBytes) {
                        return bufferedBytes;
                    }
                }
            }
        }
        return bufferedBytes;
    }

    private OptionalInt tryConvertToDirect(DictionaryColumnManager dictionaryWriter, int maxDirectBytes)
    {
        int dictionaryBytes = dictionaryWriter.getDictionaryBytes();
        OptionalInt directBytes = dictionaryWriter.tryConvertToDirect(maxDirectBytes);
        if (directBytes.isPresent()) {
            dictionaryMemoryBytes -= dictionaryBytes;
        }
        directConversionCandidates.remove(dictionaryWriter);
        return directBytes;
    }

    private double currentCompressionRatio(int totalNonDictionaryBytes)
    {
        long uncompressedBytes = totalNonDictionaryBytes;
        long compressedBytes = totalNonDictionaryBytes;

        for (DictionaryColumnManager column : allWriters) {
            if (!column.isDirectEncoded()) {
                uncompressedBytes += column.getRawBytes();
                compressedBytes += column.getDictionaryBytes();
            }
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
        checkState(!directConversionCandidates.isEmpty());

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

        for (DictionaryColumnManager column : allWriters) {
            if (!column.isDirectEncoded()) {
                totalDictionaryRawBytes += column.getRawBytes();
                totalDictionaryBytes += column.getDictionaryBytes();
                totalDictionaryIndexBytes += column.getIndexBytes();

                totalDictionaryRawBytesPerRow += column.getRawBytesPerRow();
                totalDictionaryBytesPerNewRow += column.getDictionaryBytesPerFutureRow();
                totalDictionaryIndexBytesPerRow += column.getIndexBytesPerRow();
            }
        }

        long totalUncompressedBytesPerRow = totalNonDictionaryBytesPerRow + totalDictionaryRawBytesPerRow;

        DictionaryCompressionProjection maxProjectedCompression = null;
        for (DictionaryColumnManager column : directConversionCandidates) {
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

    private int getMaxDirectBytes(int bufferedBytes)
    {
        return toIntExact(Math.min(stripeMaxBytes, stripeMaxBytes - bufferedBytes + DIRECT_COLUMN_SIZE_RANGE.toBytes()));
    }

    public interface DictionaryColumn
    {
        long getValueCount();

        long getNonNullValueCount();

        long getRawBytes();

        int getDictionaryEntries();

        int getDictionaryBytes();

        int getIndexBytes();

        OptionalInt tryConvertToDirect(int maxDirectBytes);

        long getBufferedBytes();
    }

    private static class DictionaryColumnManager
    {
        private final DictionaryColumn dictionaryColumn;
        private boolean directEncoded;

        private int rowCount;

        private long pastValueCount;
        private int pastDictionaryEntries;

        private long pendingPastValueCount;
        private int pendingPastDictionaryEntries;

        public DictionaryColumnManager(DictionaryColumn dictionaryColumn)
        {
            this.dictionaryColumn = dictionaryColumn;
        }

        OptionalInt tryConvertToDirect(int maxDirectBytes)
        {
            OptionalInt directBytes = dictionaryColumn.tryConvertToDirect(maxDirectBytes);
            if (directBytes.isPresent()) {
                directEncoded = true;
            }
            return directBytes;
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
            long currentValueCount = dictionaryColumn.getValueCount();
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
            long currentValueCount = dictionaryColumn.getValueCount();

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
            return dictionaryColumn.getIndexBytes();
        }

        public double getIndexBytesPerRow()
        {
            checkState(!directEncoded);
            return 1.0 * getIndexBytes() / rowCount;
        }

        public double getCompressionRatio()
        {
            checkState(!directEncoded);
            return 1.0 * getRawBytes() / getBufferedBytes();
        }

        public long getBufferedBytes()
        {
            return dictionaryColumn.getBufferedBytes();
        }

        public boolean isDirectEncoded()
        {
            return directEncoded;
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
