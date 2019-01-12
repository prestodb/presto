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
package io.prestosql.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.operator.window.WindowPartition;
import io.prestosql.util.Mergeable;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;

public class WindowInfo
        implements Mergeable<WindowInfo>, OperatorInfo
{
    private final List<DriverWindowInfo> windowInfos;

    @JsonCreator
    public WindowInfo(@JsonProperty("windowInfos") List<DriverWindowInfo> windowInfos)
    {
        this.windowInfos = ImmutableList.copyOf(windowInfos);
    }

    @JsonProperty
    public List<DriverWindowInfo> getWindowInfos()
    {
        return windowInfos;
    }

    @Override
    public WindowInfo mergeWith(WindowInfo other)
    {
        return new WindowInfo(ImmutableList.copyOf(concat(this.windowInfos, other.windowInfos)));
    }

    static class DriverWindowInfoBuilder
    {
        private final ImmutableList.Builder<IndexInfo> indexInfosBuilder = ImmutableList.builder();
        private IndexInfoBuilder currentIndexInfoBuilder;

        public void addIndex(PagesIndex index)
        {
            if (currentIndexInfoBuilder != null) {
                Optional<IndexInfo> indexInfo = currentIndexInfoBuilder.build();
                indexInfo.ifPresent(indexInfosBuilder::add);
            }
            currentIndexInfoBuilder = new IndexInfoBuilder(index.getPositionCount(), index.getEstimatedSize().toBytes());
        }

        public void addPartition(WindowPartition partition)
        {
            checkState(currentIndexInfoBuilder != null, "addIndex must be called before addPartition");
            currentIndexInfoBuilder.addPartition(partition);
        }

        public DriverWindowInfo build()
        {
            if (currentIndexInfoBuilder != null) {
                Optional<IndexInfo> indexInfo = currentIndexInfoBuilder.build();
                indexInfo.ifPresent(indexInfosBuilder::add);
                currentIndexInfoBuilder = null;
            }

            List<IndexInfo> indexInfos = indexInfosBuilder.build();
            if (indexInfos.size() == 0) {
                return new DriverWindowInfo(0.0, 0.0, 0.0, 0, 0, 0);
            }
            long totalRowsCount = indexInfos.stream()
                    .mapToLong(IndexInfo::getTotalRowsCount)
                    .sum();
            double averageIndexPositions = totalRowsCount / indexInfos.size();
            double squaredDifferencesPositionsOfIndex = indexInfos.stream()
                    .mapToDouble(index -> Math.pow(index.getTotalRowsCount() - averageIndexPositions, 2))
                    .sum();
            double averageIndexSize = indexInfos.stream()
                    .mapToLong(IndexInfo::getSizeInBytes)
                    .average()
                    .getAsDouble();
            double squaredDifferencesSizeOfIndex = indexInfos.stream()
                    .mapToDouble(index -> Math.pow(index.getSizeInBytes() - averageIndexSize, 2))
                    .sum();
            double squaredDifferencesSizeInPartition = indexInfos.stream()
                    .mapToDouble(IndexInfo::getSumSquaredDifferencesSizeInPartition)
                    .sum();

            long totalPartitionsCount = indexInfos.stream()
                    .mapToLong(IndexInfo::getNumberOfPartitions)
                    .sum();

            return new DriverWindowInfo(squaredDifferencesPositionsOfIndex,
                    squaredDifferencesSizeOfIndex,
                    squaredDifferencesSizeInPartition,
                    totalPartitionsCount,
                    totalRowsCount,
                    indexInfos.size());
        }
    }

    @Immutable
    public static class DriverWindowInfo
    {
        private final double sumSquaredDifferencesPositionsOfIndex; // sum of (indexPositions - averageIndexPositions) ^ 2 for all indexes
        private final double sumSquaredDifferencesSizeOfIndex; // sum of (indexSize - averageIndexSize) ^ 2 for all indexes
        private final double sumSquaredDifferencesSizeInPartition; // sum of (partitionSize - averagePartitionSize)^2 for each partition
        private final long totalPartitionsCount;
        private final long totalRowsCount;
        private final long numberOfIndexes;

        @JsonCreator
        public DriverWindowInfo(
                @JsonProperty("sumSquaredDifferencesPositionsOfIndex") double sumSquaredDifferencesPositionsOfIndex,
                @JsonProperty("sumSquaredDifferencesSizeOfIndex") double sumSquaredDifferencesSizeOfIndex,
                @JsonProperty("sumSquaredDifferencesSizeInPartition") double sumSquaredDifferencesSizeInPartition,
                @JsonProperty("totalPartitionsCount") long totalPartitionsCount,
                @JsonProperty("totalRowsCount") long totalRowsCount,
                @JsonProperty("numberOfIndexes") long numberOfIndexes)
        {
            this.sumSquaredDifferencesPositionsOfIndex = sumSquaredDifferencesPositionsOfIndex;
            this.sumSquaredDifferencesSizeOfIndex = sumSquaredDifferencesSizeOfIndex;
            this.sumSquaredDifferencesSizeInPartition = sumSquaredDifferencesSizeInPartition;
            this.totalPartitionsCount = totalPartitionsCount;
            this.totalRowsCount = totalRowsCount;
            this.numberOfIndexes = numberOfIndexes;
        }

        @JsonProperty
        public double getSumSquaredDifferencesPositionsOfIndex()
        {
            return sumSquaredDifferencesPositionsOfIndex;
        }

        @JsonProperty
        public double getSumSquaredDifferencesSizeOfIndex()
        {
            return sumSquaredDifferencesSizeOfIndex;
        }

        @JsonProperty
        public double getSumSquaredDifferencesSizeInPartition()
        {
            return sumSquaredDifferencesSizeInPartition;
        }

        @JsonProperty
        public long getTotalPartitionsCount()
        {
            return totalPartitionsCount;
        }

        @JsonProperty
        public long getTotalRowsCount()
        {
            return totalRowsCount;
        }

        @JsonProperty
        public long getNumberOfIndexes()
        {
            return numberOfIndexes;
        }
    }

    private static class IndexInfoBuilder
    {
        private final long rowsNumber;
        private final long sizeInBytes;
        private final ImmutableList.Builder<Integer> partitionsSizes = ImmutableList.builder();

        public IndexInfoBuilder(long rowsNumber, long sizeInBytes)
        {
            this.rowsNumber = rowsNumber;
            this.sizeInBytes = sizeInBytes;
        }

        public void addPartition(WindowPartition partition)
        {
            partitionsSizes.add(partition.getPartitionEnd() - partition.getPartitionStart());
        }

        public Optional<IndexInfo> build()
        {
            List<Integer> partitions = partitionsSizes.build();
            if (partitions.size() == 0) {
                return Optional.empty();
            }
            double avgSize = partitions.stream().mapToLong(Integer::longValue).average().getAsDouble();
            double squaredDifferences = partitions.stream().mapToDouble(size -> Math.pow(size - avgSize, 2)).sum();
            checkState(partitions.stream().mapToLong(Integer::longValue).sum() == rowsNumber, "Total number of rows in index does not match number of rows in partitions within that index");

            return Optional.of(new IndexInfo(rowsNumber, sizeInBytes, squaredDifferences, partitions.size()));
        }
    }

    @Immutable
    public static class IndexInfo
    {
        private final long totalRowsCount;
        private final long sizeInBytes;
        private final double sumSquaredDifferencesSizeInPartition; // sum of (partitionSize - averagePartitionSize)^2 for each partition
        private final long numberOfPartitions;

        public IndexInfo(long totalRowsCount, long sizeInBytes, double sumSquaredDifferencesSizeInPartition, long numberOfPartitions)
        {
            this.totalRowsCount = totalRowsCount;
            this.sizeInBytes = sizeInBytes;
            this.sumSquaredDifferencesSizeInPartition = sumSquaredDifferencesSizeInPartition;
            this.numberOfPartitions = numberOfPartitions;
        }

        public long getTotalRowsCount()
        {
            return totalRowsCount;
        }

        public long getSizeInBytes()
        {
            return sizeInBytes;
        }

        public double getSumSquaredDifferencesSizeInPartition()
        {
            return sumSquaredDifferencesSizeInPartition;
        }

        public long getNumberOfPartitions()
        {
            return numberOfPartitions;
        }
    }
}
