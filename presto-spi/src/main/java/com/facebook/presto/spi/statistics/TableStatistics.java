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

package com.facebook.presto.spi.statistics;

import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.HIGH;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class TableStatistics
{
    private static final TableStatistics EMPTY = TableStatistics.builder().build();

    private final Estimate rowCount;
    private final Estimate totalSize;
    private final Estimate parallelismFactor;
    private final Map<ColumnHandle, ColumnStatistics> columnStatistics;
    private ConfidenceLevel confidenceLevel;

    public static TableStatistics empty()
    {
        return EMPTY;
    }

    private TableStatistics(Estimate rowCount, Estimate totalSize, Estimate parallelismFactor, Map<ColumnHandle, ColumnStatistics> columnStatistics, ConfidenceLevel confidenceLevel)
    {
        this.rowCount = requireNonNull(rowCount, "rowCount can not be null");
        if (!rowCount.isUnknown() && rowCount.getValue() < 0) {
            throw new IllegalArgumentException(format("rowCount must be greater than or equal to 0: %s", rowCount.getValue()));
        }
        this.totalSize = requireNonNull(totalSize, "totalSize can not be null");
        if (!totalSize.isUnknown() && totalSize.getValue() < 0) {
            throw new IllegalArgumentException(format("totalSize must be greater than or equal to 0: %s", totalSize.getValue()));
        }
        this.parallelismFactor = requireNonNull(parallelismFactor, "parallelismFactor can not be null");
        if (!parallelismFactor.isUnknown() && (parallelismFactor.getValue() < 0 || parallelismFactor.getValue() > 1)) {
            throw new IllegalArgumentException(format("parallelismFactor must be between 0 and 1: %s", parallelismFactor.getValue()));
        }
        this.columnStatistics = unmodifiableMap(requireNonNull(columnStatistics, "columnStatistics can not be null"));
        this.confidenceLevel = confidenceLevel;
    }

    @JsonProperty
    public Estimate getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Estimate getTotalSize()
    {
        return totalSize;
    }

    /**
     * Returns the estimated parallelism factor for scanning this table.
     * Range: 0.0 to 1.0
     * - 0.0: Very low parallelism (few sources, may need shuffle to redistribute)
     * - 1.0: Full parallelism (enough sources to utilize all available workers)
     * - 0.5: 50% of available workers expected to be utilized
     */
    @JsonProperty
    public Estimate getParallelismFactor()
    {
        return parallelismFactor;
    }

    @JsonProperty
    public ConfidenceLevel getConfidence()
    {
        return confidenceLevel;
    }

    @JsonProperty
    public Map<ColumnHandle, ColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableStatistics that = (TableStatistics) o;
        return Objects.equals(rowCount, that.rowCount) &&
                Objects.equals(totalSize, that.totalSize) &&
                Objects.equals(parallelismFactor, that.parallelismFactor) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount, totalSize, parallelismFactor, columnStatistics);
    }

    @Override
    public String toString()
    {
        return "TableStatistics{" +
                "rowCount=" + rowCount +
                ", totalSize=" + totalSize +
                ", parallelismFactor=" + parallelismFactor +
                ", columnStatistics=" + columnStatistics +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(TableStatistics tableStatistics)
    {
        return new Builder()
                .setRowCount(tableStatistics.getRowCount())
                .setTotalSize(tableStatistics.getTotalSize())
                .setParallelismFactor(tableStatistics.getParallelismFactor())
                .setConfidenceLevel(tableStatistics.getConfidence())
                .setColumnStatistics(tableStatistics.getColumnStatistics());
    }

    public static final class Builder
    {
        private Estimate rowCount = Estimate.unknown();
        private Estimate totalSize = Estimate.unknown();
        private Estimate parallelismFactor = Estimate.unknown();
        private Map<ColumnHandle, ColumnStatistics> columnStatisticsMap = new LinkedHashMap<>();
        private ConfidenceLevel confidenceLevel = HIGH;

        public Builder setRowCount(Estimate rowCount)
        {
            this.rowCount = requireNonNull(rowCount, "rowCount can not be null");
            return this;
        }

        public Estimate getRowCount()
        {
            return rowCount;
        }

        public Builder setConfidenceLevel(ConfidenceLevel confidenceLevel)
        {
            this.confidenceLevel = confidenceLevel;
            return this;
        }

        public Builder setTotalSize(Estimate totalSize)
        {
            this.totalSize = requireNonNull(totalSize, "totalSize can not be null");
            return this;
        }

        public Builder setParallelismFactor(Estimate parallelismFactor)
        {
            this.parallelismFactor = requireNonNull(parallelismFactor, "parallelismFactor can not be null");
            return this;
        }

        public Builder setColumnStatistics(ColumnHandle columnHandle, ColumnStatistics columnStatistics)
        {
            requireNonNull(columnHandle, "columnHandle can not be null");
            requireNonNull(columnStatistics, "columnStatistics can not be null");
            this.columnStatisticsMap.put(columnHandle, columnStatistics);
            return this;
        }

        public Builder setColumnStatistics(Map<ColumnHandle, ColumnStatistics> columnStatistics)
        {
            requireNonNull(columnStatistics, "columnStatistics can not be null");
            this.columnStatisticsMap.putAll(columnStatistics);
            return this;
        }

        public Map<ColumnHandle, ColumnStatistics> getColumnStatistics()
        {
            return Collections.unmodifiableMap(columnStatisticsMap);
        }

        public TableStatistics build()
        {
            return new TableStatistics(rowCount, totalSize, parallelismFactor, columnStatisticsMap, confidenceLevel);
        }
    }
}
