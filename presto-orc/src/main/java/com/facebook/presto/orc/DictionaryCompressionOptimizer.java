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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * DictionaryCompressionOptimizer has 2 objectives:
 * 1) Bound the dictionary memory of the reader, when all columns are read. Reader's dictionary memory
 * should not exceed the dictionaryMemoryMaxBytesHigh.
 * 2) When dictionary encoding for a column produces size comparable to the direct encoding, choose
 * direct encoding over dictionary encoding. Dictionary encoding/decoding is memory and CPU intensive,
 * so for comparable column sizes, direct encoding is mostly better.
 * <p>
 * Note: Dictionary writer might use more memory as they over-allocate dictionary sizes as the writers
 * build dictionary as they see new data. The hash tables implementation in the dictionary writer's allocate
 * hash buckets in power of 2. So after a million entries, the overallocation consumes large amount of memory.
 * <p>
 * DictionaryCompressionOptimizer functionality can be controlled by the following configs to the constructor.
 * <p>
 * 1. dictionaryMemoryMaxBytes -> Max size of the dictionary when all columns are read. Note: Writer
 * might consume more memory due to the over-allocation.
 * <p>
 * 2. dictionaryMemoryAlmostFullRangeBytes -> When the dictionary size exceeds dictionaryMaxMemoryBytes
 * dictionary columns will be converted to direct to reduce the dictionary size. By setting a range
 * the stripe can be flushed, before the dictionary is full. When dictionary size is higher than
 * (dictionaryMemoryMaxBytes - dictionaryMemoryAlmostFullRangeBytes), it is considered almost full
 * and is ready for flushing. This setting is defined as a delta on dictionaryMemoryMaxBytes for backward compatibility.
 * <p>
 * 3. dictionaryUsefulCheckColumnSizeBytes -> Columns start with dictionary encoding and when the dictionary memory
 * is almost full, usefulness of the dictionary is measured. For large dictionaries (> 40 MB) the check
 * might happen very late and large dictionary might cause writer to OOM due to writer over allocating for
 * dictionary growth. When a dictionary for a column grows beyond the dictionaryUsefulCheckColumnSizeBytes the
 * dictionary usefulness check will be performed and if dictionary is not useful, it will be converted to direct.
 * <p>
 * 4. dictionaryUsefulCheckPerChunkFrequency -> dictionaryUsefulCheck could be costly if performed on every chunk.
 * The dictionaryUsefulCheck will be performed when a column dictionary is above the dictionaryUsefulCheckColumnSizeBytes
 * and per every dictionaryUsefulCheckPerChunkFrequency chunks written.
 */
public class DictionaryCompressionOptimizer
{
    private static final double DICTIONARY_MIN_COMPRESSION_RATIO = 1.25;

    static final DataSize DIRECT_COLUMN_SIZE_RANGE = new DataSize(4, MEGABYTE);

    private final List<DictionaryColumnManager> allWriters;
    private final List<DictionaryColumnManager> directConversionCandidates = new ArrayList<>();

    private final int stripeMinBytes;
    private final int stripeMaxBytes;
    private final int stripeMaxRowCount;
    private final int dictionaryMemoryMaxBytesLow;
    private final int dictionaryMemoryMaxBytesHigh;
    private final int dictionaryUsefulCheckColumnSizeBytes;
    private final int dictionaryUsefulCheckPerChunkFrequency;

    private int dictionaryMemoryBytes;
    private int dictionaryUsefulCheckCounter;

    public DictionaryCompressionOptimizer(
            Set<? extends DictionaryColumn> writers,
            int stripeMinBytes,
            int stripeMaxBytes,
            int stripeMaxRowCount,
            int dictionaryMemoryMaxBytes,
            int dictionaryMemoryAlmostFullRangeBytes,
            int dictionaryUsefulCheckColumnSizeBytes,
            int dictionaryUsefulCheckPerChunkFrequency)
    {
        requireNonNull(writers, "writers is null");
        this.allWriters = writers.stream()
                .map(DictionaryColumnManager::new)
                .collect(toImmutableList());

        checkArgument(stripeMinBytes >= 0, "stripeMinBytes is negative");
        this.stripeMinBytes = stripeMinBytes;

        checkArgument(stripeMaxBytes >= stripeMinBytes, "stripeMaxBytes is less than stripeMinBytes");
        this.stripeMaxBytes = stripeMaxBytes;

        checkArgument(stripeMaxRowCount >= 0, "stripeMaxRowCount is negative");
        this.stripeMaxRowCount = stripeMaxRowCount;

        checkArgument(dictionaryMemoryMaxBytes >= 0, "dictionaryMemoryMaxBytes is negative");
        checkArgument(dictionaryMemoryAlmostFullRangeBytes >= 0, "dictionaryMemoryRangeBytes is negative");
        this.dictionaryMemoryMaxBytesHigh = dictionaryMemoryMaxBytes;
        this.dictionaryMemoryMaxBytesLow = Math.max(dictionaryMemoryMaxBytes - dictionaryMemoryAlmostFullRangeBytes, 0);

        checkArgument(dictionaryUsefulCheckPerChunkFrequency >= 0, "dictionaryUsefulCheckPerChunkFrequency is negative");
        this.dictionaryUsefulCheckPerChunkFrequency = dictionaryUsefulCheckPerChunkFrequency;

        this.dictionaryUsefulCheckColumnSizeBytes = dictionaryUsefulCheckColumnSizeBytes;
        directConversionCandidates.addAll(allWriters);
    }

    public int getDictionaryMemoryBytes()
    {
        return dictionaryMemoryBytes;
    }

    public boolean isFull(long bufferedBytes)
    {
        // if the stripe is big enough to flush, stop before we hit the absolute max, so we are
        // not forced to convert a dictionary to direct to fit in memory
        if (bufferedBytes > stripeMinBytes) {
            return dictionaryMemoryBytes > dictionaryMemoryMaxBytesLow;
        }
        // stripe is small, grow to the high watermark (so at the very least we have more information)
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
        updateDirectConversionCandidates();
        convertLowCompressionStreams(true, bufferedBytes);
    }

    @VisibleForTesting
    boolean isUsefulCheckRequired(int dictionaryMemoryBytes)
    {
        if (dictionaryMemoryBytes < dictionaryUsefulCheckColumnSizeBytes) {
            return false;
        }

        dictionaryUsefulCheckCounter++;
        if (dictionaryUsefulCheckCounter == dictionaryUsefulCheckPerChunkFrequency) {
            dictionaryUsefulCheckCounter = 0;
            return true;
        }

        return false;
    }

    public void optimize(int bufferedBytes, int stripeRowCount)
    {
        // recompute the dictionary memory usage
        int totalDictionaryBytes = 0;
        for (DictionaryColumnManager writer : allWriters) {
            if (!writer.isDirectEncoded()) {
                totalDictionaryBytes += writer.getDictionaryBytes();
                writer.updateHistory(stripeRowCount);
            }
        }
        dictionaryMemoryBytes = totalDictionaryBytes;

        boolean isDictionaryAlmostFull = dictionaryMemoryBytes > dictionaryMemoryMaxBytesLow;

        if (isDictionaryAlmostFull || isUsefulCheckRequired(dictionaryMemoryBytes)) {
            updateDirectConversionCandidates();
            bufferedBytes = convertLowCompressionStreams(isDictionaryAlmostFull, bufferedBytes);
        }

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

        BufferedBytesCounter bufferedBytesCounter = new BufferedBytesCounter(bufferedBytes, nonDictionaryBufferedBytes);
        optimizeDictionaryColumns(stripeRowCount, bufferedBytesCounter);
    }

    private void optimizeDictionaryColumns(int stripeRowCount, BufferedBytesCounter bufferedBytesCounter)
    {
        // convert dictionary columns to direct until we are below the high memory limit
        while (!directConversionCandidates.isEmpty()
                && dictionaryMemoryBytes > dictionaryMemoryMaxBytesHigh
                && bufferedBytesCounter.getBufferedBytes() < stripeMaxBytes) {
            convertDictionaryColumn(bufferedBytesCounter, stripeRowCount, OptionalDouble.empty());
        }

        if (bufferedBytesCounter.getBufferedBytes() >= stripeMaxBytes) {
            return;
        }

        // if the stripe is larger than the minimum stripe size, we are not required to convert any more dictionary columns to direct
        if (bufferedBytesCounter.getBufferedBytes() >= stripeMinBytes) {
            // check if we can get better compression by converting a dictionary column to direct.  This can happen when then there are multiple
            // dictionary columns and one does not compress well, so if we convert it to direct we can continue to use the existing dictionaries
            // for the other columns.
            double currentCompressionRatio = currentCompressionRatio(bufferedBytesCounter.getNonDictionaryBufferedBytes());
            while (!directConversionCandidates.isEmpty() && bufferedBytesCounter.getBufferedBytes() < stripeMaxBytes) {
                if (!convertDictionaryColumn(bufferedBytesCounter, stripeRowCount, OptionalDouble.of(currentCompressionRatio))) {
                    return;
                }
            }
        }
    }

    private boolean convertDictionaryColumn(BufferedBytesCounter bufferedBytesCounter, int stripeRowCount, OptionalDouble currentCompressionRatio)
    {
        DictionaryCompressionProjection projection = selectDictionaryColumnToConvert(bufferedBytesCounter.getNonDictionaryBufferedBytes(), stripeRowCount);
        int index = projection.getDirectConversionCandidateIndex();
        if (currentCompressionRatio.isPresent() && projection.getPredictedFileCompressionRatio() < currentCompressionRatio.getAsDouble()) {
            return false;
        }

        DictionaryColumnManager column = directConversionCandidates.get(index);
        int dictionaryBytes = toIntExact(column.getBufferedBytes());

        OptionalInt directBytes = tryConvertToDirect(column, getMaxDirectBytes(bufferedBytesCounter.getBufferedBytes()));
        removeDirectConversionCandidate(index);
        if (directBytes.isPresent()) {
            bufferedBytesCounter.incrementBufferedBytes(directBytes.getAsInt() - dictionaryBytes);
            bufferedBytesCounter.incrementNonDictionaryBufferedBytes(directBytes.getAsInt());
        }
        return true;
    }

    @VisibleForTesting
    int convertLowCompressionStreams(boolean tryAllStreams, int bufferedBytes)
    {
        // convert all low compression column to direct
        Iterator<DictionaryColumnManager> iterator = directConversionCandidates.iterator();
        while (iterator.hasNext()) {
            DictionaryColumnManager dictionaryWriter = iterator.next();
            if (tryAllStreams || dictionaryWriter.getDictionaryBytes() >= dictionaryUsefulCheckColumnSizeBytes) {
                if (dictionaryWriter.getCompressionRatio() < DICTIONARY_MIN_COMPRESSION_RATIO) {
                    int columnBufferedBytes = toIntExact(dictionaryWriter.getBufferedBytes());
                    OptionalInt directBytes = tryConvertToDirect(dictionaryWriter, getMaxDirectBytes(bufferedBytes));
                    iterator.remove();
                    if (directBytes.isPresent()) {
                        bufferedBytes = bufferedBytes + directBytes.getAsInt() - columnBufferedBytes;
                        if (bufferedBytes >= stripeMaxBytes) {
                            return bufferedBytes;
                        }
                    }
                }
            }
        }
        return bufferedBytes;
    }

    @VisibleForTesting
    List<DictionaryColumnManager> getDirectConversionCandidates()
    {
        return directConversionCandidates;
    }

    private void updateDirectConversionCandidates()
    {
        // Writers can switch to Direct encoding internally. Remove them from direct conversion candidates.
        directConversionCandidates.removeIf(DictionaryColumnManager::isDirectEncoded);
    }

    private void removeDirectConversionCandidate(int index)
    {
        DictionaryColumnManager last = directConversionCandidates.get(directConversionCandidates.size() - 1);
        directConversionCandidates.set(index, last);
        directConversionCandidates.remove(directConversionCandidates.size() - 1);
    }

    private OptionalInt tryConvertToDirect(DictionaryColumnManager dictionaryWriter, int maxDirectBytes)
    {
        int dictionaryBytes = dictionaryWriter.getDictionaryBytes();
        OptionalInt directBytes = dictionaryWriter.tryConvertToDirect(maxDirectBytes);
        if (directBytes.isPresent()) {
            dictionaryMemoryBytes -= dictionaryBytes;
        }
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
     * Choose a dictionary column to convert to direct encoding.  We do this by predicting the compression ratio
     * of the stripe if a singe column is flipped to direct.  So for each column, we try to predict the row count
     * when we will hit a stripe flush limit if that column were converted to direct.  Once we know the row count, we
     * calculate the predicted compression ratio.
     *
     * @param totalNonDictionaryBytes current size of the stripe without non-dictionary columns
     * @param stripeRowCount current number of rows in the stripe
     * @return the column that would produce the best stripe compression ratio if converted to direct
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
        for (int index = 0; index < directConversionCandidates.size(); index++) {
            DictionaryColumnManager column = directConversionCandidates.get(index);
            // determine the size of the currently written stripe if we were convert this column to direct
            long currentRawBytes = totalNonDictionaryBytes + column.getRawBytes();
            long currentDictionaryBytes = totalDictionaryBytes - column.getDictionaryBytes();
            long currentIndexBytes = totalDictionaryIndexBytes - column.getIndexBytes();
            long currentTotalBytes = currentRawBytes + currentDictionaryBytes + currentIndexBytes;

            // estimate the size of each new row if we were to convert this column to direct
            double rawBytesPerFutureRow = totalNonDictionaryBytesPerRow + column.getRawBytesPerRow();
            double dictionaryBytesPerFutureRow = totalDictionaryBytesPerNewRow - column.getDictionaryBytesPerFutureRow();
            double indexBytesPerFutureRow = totalDictionaryIndexBytesPerRow - column.getIndexBytesPerRow();
            double totalBytesPerFutureRow = rawBytesPerFutureRow + dictionaryBytesPerFutureRow + indexBytesPerFutureRow;

            // estimate how many rows until we hit a limit and flush the stripe if we convert this column to direct
            long rowsToDictionaryMemoryLimit = (long) ((dictionaryMemoryMaxBytesLow - currentDictionaryBytes) / dictionaryBytesPerFutureRow);
            long rowsToStripeMemoryLimit = (long) ((stripeMaxBytes - currentTotalBytes) / totalBytesPerFutureRow);
            long rowsToStripeRowLimit = stripeMaxRowCount - stripeRowCount;
            long rowsToLimit = Longs.min(rowsToDictionaryMemoryLimit, rowsToStripeMemoryLimit, rowsToStripeRowLimit);

            // predict the compression ratio at that limit if we were to convert this column to direct
            long predictedUncompressedSizeAtLimit = totalNonDictionaryBytes + totalDictionaryRawBytes + (totalUncompressedBytesPerRow * rowsToLimit);
            long predictedCompressedSizeAtLimit = (long) (currentTotalBytes + (totalBytesPerFutureRow * rowsToLimit));
            double predictedCompressionRatioAtLimit = 1.0 * predictedUncompressedSizeAtLimit / predictedCompressedSizeAtLimit;

            // convert the column that creates the best compression ratio
            if (maxProjectedCompression == null || maxProjectedCompression.getPredictedFileCompressionRatio() < predictedCompressionRatioAtLimit) {
                maxProjectedCompression = new DictionaryCompressionProjection(index, predictedCompressionRatioAtLimit);
            }
        }
        return maxProjectedCompression;
    }

    private int getMaxDirectBytes(int bufferedBytes)
    {
        return toIntExact(Math.min(stripeMaxBytes, stripeMaxBytes - bufferedBytes + DIRECT_COLUMN_SIZE_RANGE.toBytes()));
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
        long getValueCount();

        long getNonNullValueCount();

        long getRawBytes();

        int getDictionaryEntries();

        int getDictionaryBytes();

        int getIndexBytes();

        OptionalInt tryConvertToDirect(int maxDirectBytes);

        long getBufferedBytes();

        boolean isDirectEncoded();
    }

    @VisibleForTesting
    static class DictionaryColumnManager
    {
        private final DictionaryColumn dictionaryColumn;

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
            return dictionaryColumn.tryConvertToDirect(maxDirectBytes);
        }

        void reset()
        {
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
            checkState(!isDirectEncoded());
            return dictionaryColumn.getRawBytes();
        }

        public double getRawBytesPerRow()
        {
            checkState(!isDirectEncoded());
            return 1.0 * getRawBytes() / rowCount;
        }

        public int getDictionaryBytes()
        {
            checkState(!isDirectEncoded());
            return dictionaryColumn.getDictionaryBytes();
        }

        public double getDictionaryBytesPerFutureRow()
        {
            checkState(!isDirectEncoded());

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
            checkState(!isDirectEncoded());
            return dictionaryColumn.getIndexBytes();
        }

        public double getIndexBytesPerRow()
        {
            checkState(!isDirectEncoded());
            return 1.0 * getIndexBytes() / rowCount;
        }

        public double getCompressionRatio()
        {
            checkState(!isDirectEncoded());
            long bufferedBytes = getBufferedBytes();
            if (bufferedBytes == 0) {
                return 0;
            }
            return 1.0 * getRawBytes() / bufferedBytes;
        }

        public long getBufferedBytes()
        {
            return dictionaryColumn.getBufferedBytes();
        }

        public boolean isDirectEncoded()
        {
            return dictionaryColumn.isDirectEncoded();
        }

        @VisibleForTesting
        public DictionaryColumn getDictionaryColumn()
        {
            return dictionaryColumn;
        }
    }

    private static class DictionaryCompressionProjection
    {
        private final int directConversionIndex;
        private final double predictedFileCompressionRatio;

        public DictionaryCompressionProjection(int directConversionIndex, double predictedFileCompressionRatio)
        {
            this.directConversionIndex = directConversionIndex;
            this.predictedFileCompressionRatio = predictedFileCompressionRatio;
        }

        public int getDirectConversionCandidateIndex()
        {
            return directConversionIndex;
        }

        public double getPredictedFileCompressionRatio()
        {
            return predictedFileCompressionRatio;
        }
    }

    private static class BufferedBytesCounter
    {
        private int bufferedBytes;
        private int nonDictionaryBufferedBytes;

        public BufferedBytesCounter(int bufferedBytes, int nonDictionaryBufferedBytes)
        {
            this.bufferedBytes = bufferedBytes;
            this.nonDictionaryBufferedBytes = nonDictionaryBufferedBytes;
        }

        public int getBufferedBytes()
        {
            return bufferedBytes;
        }

        public void incrementBufferedBytes(int value)
        {
            bufferedBytes += value;
        }

        public int getNonDictionaryBufferedBytes()
        {
            return nonDictionaryBufferedBytes;
        }

        public void incrementNonDictionaryBufferedBytes(int value)
        {
            nonDictionaryBufferedBytes += value;
        }
    }
}
