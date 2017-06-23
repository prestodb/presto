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

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class DictionaryCompressionOptimizer
{
    private static final double DICTIONARY_MIN_COMPRESSION_RATIO = 1.25;
    private static final double DICTIONARY_ALWAYS_KEEP_COMPRESSION_RATIO = 3.0;

    private final Set<DictionaryColumnManager> allWriters;
    private final Set<DictionaryColumnManager> dictionaryWriters = new HashSet<>();

    private final int stripeMaxBytes;
    private final int stripeMinRowCount;
    private final int stripeMaxRowCount;
    private final int dictionaryMemoryMaxBytes;

    private int dictionaryMemoryBytes;

    public DictionaryCompressionOptimizer(
            Set<? extends DictionaryColumn> writers,
            int stripeMaxBytes,
            int stripeMinRowCount,
            int stripeMaxRowCount,
            int dictionaryMemoryMaxBytes)
    {
        this.allWriters = ImmutableSet.copyOf(writers.stream()
                .map(DictionaryColumnManager::new)
                .collect(toSet()));
        this.stripeMaxBytes = stripeMaxBytes;
        this.stripeMinRowCount = stripeMinRowCount;
        this.stripeMaxRowCount = stripeMaxRowCount;
        this.dictionaryMemoryMaxBytes = dictionaryMemoryMaxBytes;

        dictionaryWriters.addAll(allWriters);
    }

    public boolean isFull()
    {
        return dictionaryMemoryBytes > dictionaryMemoryMaxBytes;
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

        if (dictionaryMemoryBytes <= dictionaryMemoryMaxBytes) {
            return;
        }

        // before any further checks, convert all low compression streams
        convertLowCompressionStreams();

        if (dictionaryMemoryBytes <= dictionaryMemoryMaxBytes) {
            return;
        }

        // all other bytes are the buffered bytes without the dictionary columns
        int allOtherBytes = bufferedBytes;
        for (DictionaryColumnManager dictionaryWriter : dictionaryWriters) {
            allOtherBytes -= dictionaryWriter.getBufferedBytes();
        }

        // if the stripe is larger then the minimum row count, we are not required to convert any dictionary columns to direct
        if (stripeRowCount >= stripeMinRowCount) {
            // check if we can get better compression by converting a dictionary column to direct.  This can happen when the effective dictionary columns
            // are much wider than the ineffective dictionary columns.
            double currentCompressionRatio = currentCompressionRatio(allOtherBytes);
            while (!dictionaryWriters.isEmpty()) {
                DictionaryCompressionProjection projection = selectDictionaryColumnToConvert(allOtherBytes, stripeRowCount);
                if (projection.getPredictedFileCompressionRatio() < currentCompressionRatio) {
                    return;
                }
                convertToDirect(projection.getColumnToConvert());
            }
        }

        // convert dictionary columns to direct until we are blow the memory limit
        while (dictionaryMemoryBytes > dictionaryMemoryMaxBytes) {
            DictionaryCompressionProjection projection = selectDictionaryColumnToConvert(allOtherBytes, stripeRowCount);
            if (projection.getColumnToConvert().getCompressionRatio() >= DICTIONARY_ALWAYS_KEEP_COMPRESSION_RATIO) {
                return;
            }
            convertToDirect(projection.getColumnToConvert());
        }
    }

    private void convertLowCompressionStreams()
    {
        // convert all low compression column to direct
        ImmutableList.copyOf(dictionaryWriters).stream()
                .filter(dictionaryWriter -> dictionaryWriter.getCompressionRatio() < DICTIONARY_MIN_COMPRESSION_RATIO)
                .forEach(this::convertToDirect);
    }

    private void convertToDirect(DictionaryColumnManager dictionaryWriter)
    {
        dictionaryMemoryBytes -= dictionaryWriter.getDictionaryBytes();
        dictionaryWriter.convertToDirect();
        dictionaryWriters.remove(dictionaryWriter);
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

    private DictionaryCompressionProjection selectDictionaryColumnToConvert(int allOtherBytes, int stripeRowCount)
    {
        checkState(!dictionaryWriters.isEmpty());

        int allOthersBytesPerRow = allOtherBytes / stripeRowCount;

        long totalRawBytes = 0;
        long totalDictionaryBytes = 0;
        long totalIndexBytes = 0;

        int totalDictionaryBytesPerNewRow = 0;
        int totalIndexBytesPerRow = 0;
        int totalRawBytesPerRow = 0;

        for (DictionaryColumnManager column : dictionaryWriters) {
            totalRawBytes += column.getRawBytes();
            totalDictionaryBytes += column.getDictionaryBytes();
            totalIndexBytes += column.getIndexBytes();

            totalRawBytesPerRow += column.getRawBytesPerRow();
            totalDictionaryBytesPerNewRow += column.getDictionaryBytesPerFutureRow();
            totalIndexBytesPerRow += column.getIndexBytesPerRow();
        }

        DictionaryCompressionProjection maxProjectedCompression = null;
        for (DictionaryColumnManager column : dictionaryWriters) {
            // determine the current size if we were convert this column to direct
            long currentRawBytes = allOtherBytes + column.getRawBytes();
            long currentDictionaryBytes = totalDictionaryBytes - column.getDictionaryBytes();
            long currentIndexBytes = totalIndexBytes - column.getIndexBytes();
            long currentTotalBytes = currentRawBytes + currentDictionaryBytes + currentIndexBytes;

            // estimate the size of each new row if we were convert this column to direct
            double rawBytesPerRow = allOthersBytesPerRow + column.getRawBytesPerRow();
            double dictionaryBytesPerRow = totalDictionaryBytesPerNewRow - column.getDictionaryBytesPerFutureRow();
            double indexBytesPerRow = totalIndexBytesPerRow - column.getIndexBytesPerRow();
            double totalBytesPerRow = rawBytesPerRow + dictionaryBytesPerRow + indexBytesPerRow;

            // estimate how many rows until we hit either the memory or stripe limit if we were convert this column to direct
            long rowsToMemoryLimit = (int) Math.min((dictionaryMemoryMaxBytes - currentDictionaryBytes) / dictionaryBytesPerRow, stripeMaxRowCount - stripeRowCount);
            long rowsToStripeLimit = (int) Math.min((stripeMaxBytes - currentTotalBytes) / totalBytesPerRow, stripeMaxRowCount - stripeRowCount);
            long rowsToLimit = Math.min(rowsToMemoryLimit, rowsToStripeLimit);

            // predict the compression ratio at that limit if we were convert this column to direct
            long predictedUncompressedSizeAtLimit = allOtherBytes + totalRawBytes + ((allOthersBytesPerRow + totalRawBytesPerRow) * rowsToLimit);
            long predictedCompressedSizeAtLimit = (long) (currentTotalBytes + (totalBytesPerRow * rowsToLimit));
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
        return (31 - Integer.numberOfLeadingZeros(dictionaryEntries) + 8) / 8;
    }

    public interface DictionaryColumn
    {
        int getValueCount();

        int getNonNullValueCount();

        long getRawBytes();

        int getDictionaryEntries();

        int getDictionaryBytes();

        void convertToDirect();
    }

    private static class DictionaryColumnManager
    {
        private final DictionaryColumn dictionaryColumn;
        private boolean directEncoded;

        private int rowCount;

        private int pastValueCount;
        private int pastDictionaryEntries;

        private int nextPastValueCount;
        private int nextPastDictionaryEntries;

        public DictionaryColumnManager(DictionaryColumn dictionaryColumn)
        {
            this.dictionaryColumn = dictionaryColumn;
        }

        void convertToDirect()
        {
            directEncoded = true;
            dictionaryColumn.convertToDirect();
        }

        void reset()
        {
            directEncoded = false;

            pastValueCount = 0;
            pastDictionaryEntries = 0;

            nextPastValueCount = 0;
            nextPastDictionaryEntries = 0;
        }

        public void updateHistory(int rowCount)
        {
            this.rowCount = rowCount;
            int currentValueCount = dictionaryColumn.getValueCount();
            if (currentValueCount - nextPastValueCount >= 1024) {
                pastValueCount = nextPastValueCount;
                pastDictionaryEntries = nextPastDictionaryEntries;

                nextPastValueCount = currentValueCount;
                nextPastDictionaryEntries = dictionaryColumn.getDictionaryEntries();
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
            int dictionaryEntries = dictionaryColumn.getDictionaryEntries();
            double dictionaryBytesPerEntry = 1.0 * dictionaryColumn.getDictionaryBytes() / dictionaryEntries;
            double dictionaryBytesPerFutureValue = (1.0 * (dictionaryEntries - pastDictionaryEntries) / (dictionaryColumn.getValueCount() - pastValueCount)) * dictionaryBytesPerEntry;
            return dictionaryBytesPerFutureValue * dictionaryColumn.getNonNullValueCount() / rowCount;
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
