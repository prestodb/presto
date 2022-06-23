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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.predicate.TupleDomainParquetPredicate;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ColumnIndexFilterUtils
{
    private ColumnIndexFilterUtils() {}

    static class OffsetRange
    {
        private final long offset;
        private long length;

        public OffsetRange(long offset, int length)
        {
            this.offset = offset;
            this.length = length;
        }

        long getOffset()
        {
            return offset;
        }

        long getLength()
        {
            return length;
        }

        private boolean extendWithCheck(long offset, int length)
        {
            if (this.offset + this.length == offset) {
                this.length += length;
                return true;
            }
            else {
                return false;
            }
        }

        public void extendLength(long length)
        {
            this.length += length;
        }

        public long endPos()
        {
            return offset + length;
        }
    }

    private static class FilteredOffsetIndex
            implements OffsetIndex
    {
        private final OffsetIndex offsetIndex;
        private final int[] indices;

        private FilteredOffsetIndex(OffsetIndex offsetIndex, int[] indices)
        {
            this.offsetIndex = offsetIndex;
            this.indices = indices;
        }

        @Override
        public int getPageCount()
        {
            return indices.length;
        }

        @Override
        public long getOffset(int pageIndex)
        {
            return offsetIndex.getOffset(indices[pageIndex]);
        }

        @Override
        public int getCompressedPageSize(int pageIndex)
        {
            return offsetIndex.getCompressedPageSize(indices[pageIndex]);
        }

        @Override
        public long getFirstRowIndex(int pageIndex)
        {
            return offsetIndex.getFirstRowIndex(indices[pageIndex]);
        }

        @Override
        public long getLastRowIndex(int pageIndex, long totalRowCount)
        {
            int nextIndex = indices[pageIndex] + 1;
            return (nextIndex >= offsetIndex.getPageCount() ? totalRowCount : offsetIndex.getFirstRowIndex(nextIndex)) - 1;
        }

        @Override
        public String toString()
        {
            try (Formatter formatter = new Formatter()) {
                formatter.format("%-12s  %20s  %16s  %20s\n", "", "offset", "compressed size", "first row index");
                for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
                    int index = Arrays.binarySearch(indices, i);
                    boolean isHidden = index < 0;
                    formatter.format("%spage-%-5d  %20d  %16d  %20d\n",
                            isHidden ? "- " : "  ",
                            isHidden ? i : index,
                            offsetIndex.getOffset(i),
                            offsetIndex.getCompressedPageSize(i),
                            offsetIndex.getFirstRowIndex(i));
                }
                return formatter.toString();
            }
        }
    }

    /*
     * Returns the filtered offset index containing only the pages which are overlapping with rowRanges.
     */
    static OffsetIndex filterOffsetIndex(OffsetIndex offsetIndex, RowRanges rowRanges, long totalRowCount)
    {
        IntList indices = new IntArrayList();
        for (int i = 0; i < offsetIndex.getPageCount(); i++) {
            long from = offsetIndex.getFirstRowIndex(i);
            if (rowRanges.isOverlapping(from, offsetIndex.getLastRowIndex(i, totalRowCount))) {
                indices.add(i);
            }
        }
        return new FilteredOffsetIndex(offsetIndex, indices.toIntArray());
    }

    static List<OffsetRange> calculateOffsetRanges(OffsetIndex offsetIndex, ColumnChunkMetaData columnChunkMetadata, long firstPageOffset, long startingPosition)
    {
        List<OffsetRange> ranges = new ArrayList<>();
        int pageCount = offsetIndex.getPageCount();
        if (pageCount > 0) {
            OffsetRange currentRange = null;

            // Add a range for the dictionary page if required
            long rowGroupOffset = columnChunkMetadata.getStartingPos();
            if (rowGroupOffset < firstPageOffset) {
                // We need to adjust the offset by startingPosition for presto because dataSource.readFully() started at startingPosition
                currentRange = new OffsetRange(rowGroupOffset - startingPosition, (int) (firstPageOffset - rowGroupOffset));
                ranges.add(currentRange);
            }

            for (int i = 0; i < pageCount; i++) {
                long offset = offsetIndex.getOffset(i);
                int length = offsetIndex.getCompressedPageSize(i);
                // We need to adjust the offset by startingPosition for presto because dataSource.readFully() started at startingPosition
                if (currentRange == null || !currentRange.extendWithCheck(offset - startingPosition, length)) {
                    currentRange = new OffsetRange(offset - startingPosition, length);
                    ranges.add(currentRange);
                }
            }
        }
        return ranges;
    }

    public static Optional<ColumnIndexStore> getColumnIndexStore(Predicate parquetPredicate, ParquetDataSource dataSource, BlockMetaData blockMetadata, Map<List<String>, RichColumnDescriptor> descriptorsByPath, boolean columnIndexFilterEnabled)
    {
        if (!columnIndexFilterEnabled || parquetPredicate == null || !(parquetPredicate instanceof TupleDomainParquetPredicate)) {
            return Optional.empty();
        }

        for (ColumnChunkMetaData column : blockMetadata.getColumns()) {
            if (column.getColumnIndexReference() != null && column.getOffsetIndexReference() != null) {
                Set<ColumnPath> paths = new HashSet<>();
                for (List<String> path : descriptorsByPath.keySet()) {
                    paths.add(ColumnPath.get(path.toArray(new String[0])));
                }
                return Optional.of(ParquetColumnIndexStore.create(dataSource, blockMetadata, paths));
            }
        }
        return Optional.empty();
    }
}
