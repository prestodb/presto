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
package com.facebook.presto.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.openjdk.jol.info.ClassLayout;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class Partition
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(Partition.class).instanceSize();
    // A boxed field id, a boxed value or CharBuffer, and the map's own node. Deliberately one rough
    // constant: the values are Long, Double or CharBuffer, and sizing each one exactly would cost
    // more than a cache bound is worth.
    private static final long ENTRY_SIZE = 64;

    private final Map<Integer, Type.PrimitiveType> idToTypeMapping;
    private final List<Types.NestedField> nonPartitionPrimitiveColumns;
    private final StructLike values;
    private long recordCount;
    private long fileCount;
    private long size;
    private final Map<Integer, Object> minValues;
    private final Map<Integer, Object> maxValues;
    private final Map<Integer, Long> nullCounts;
    private final Map<Integer, Long> columnSizes;
    private final Set<Integer> corruptedStats;
    private boolean hasValidColumnMetrics;

    public Partition(
            Map<Integer, Type.PrimitiveType> idToTypeMapping,
            List<Types.NestedField> nonPartitionPrimitiveColumns,
            StructLike values,
            long recordCount,
            long size,
            Map<Integer, Object> minValues,
            Map<Integer, Object> maxValues,
            Map<Integer, Long> nullCounts,
            Map<Integer, Long> columnSizes)
    {
        this.idToTypeMapping = ImmutableMap.copyOf(requireNonNull(idToTypeMapping, "idToTypeMapping is null"));
        this.nonPartitionPrimitiveColumns = ImmutableList.copyOf(requireNonNull(nonPartitionPrimitiveColumns, "nonPartitionPrimitiveColumns is null"));
        this.values = requireNonNull(values, "values is null");
        this.recordCount = recordCount;
        this.fileCount = 1;
        this.size = size;
        if (minValues == null || maxValues == null || nullCounts == null) {
            this.minValues = null;
            this.maxValues = null;
            this.nullCounts = null;
            this.columnSizes = null;
            corruptedStats = null;
        }
        else {
            this.minValues = new HashMap<>(minValues);
            this.maxValues = new HashMap<>(maxValues);
            // we are assuming if minValues is not present, max will be not be present either.
            this.corruptedStats = nonPartitionPrimitiveColumns.stream()
                    .map(Types.NestedField::fieldId)
                    .filter(id -> !minValues.containsKey(id) && (!nullCounts.containsKey(id) || nullCounts.get(id) != recordCount))
                    .collect(toSet());
            this.nullCounts = new HashMap<>(nullCounts);
            this.columnSizes = columnSizes != null ? new HashMap<>(columnSizes) : null;
            hasValidColumnMetrics = true;
        }
    }

    private Partition(Partition source)
    {
        this.idToTypeMapping = source.idToTypeMapping;
        this.nonPartitionPrimitiveColumns = source.nonPartitionPrimitiveColumns;
        this.values = source.values;
        this.recordCount = source.recordCount;
        this.fileCount = source.fileCount;
        this.size = source.size;
        this.minValues = copyUnmodifiable(source.minValues);
        this.maxValues = copyUnmodifiable(source.maxValues);
        this.nullCounts = copyUnmodifiable(source.nullCounts);
        this.columnSizes = copyUnmodifiable(source.columnSizes);
        this.corruptedStats = source.corruptedStats == null ? null : unmodifiableSet(new HashSet<>(source.corruptedStats));
        this.hasValidColumnMetrics = source.hasValidColumnMetrics;
    }

    private static <T> Map<Integer, T> copyUnmodifiable(Map<Integer, T> values)
    {
        return values == null ? null : unmodifiableMap(new HashMap<>(values));
    }

    /**
     * Returns a copy whose maps and sets reject mutation.
     *
     * <p>A {@code Partition} is a fold accumulator: {@link #updateStats} is called with
     * {@link #getMinValues()} as its target, so the getters hand out live mutation handles and the
     * object is only complete once the fold that built it has finished. Publishing one to a cache
     * shared across queries therefore needs this copy — otherwise a caller that mutates what a
     * getter returned would silently corrupt the statistics of every later query that hits the same
     * entry. With the copy that same call fails loudly instead.
     */
    public Partition immutableCopy()
    {
        return new Partition(this);
    }

    /**
     * Rough retained size, for weighing cache entries. Grows with the table's column count, which is
     * the whole reason a cache of these cannot be bounded by entry count alone.
     */
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                (idToTypeMapping.size() + nonPartitionPrimitiveColumns.size()) * ENTRY_SIZE +
                estimatedSizeOf(minValues) +
                estimatedSizeOf(maxValues) +
                estimatedSizeOf(nullCounts) +
                estimatedSizeOf(columnSizes) +
                (corruptedStats == null ? 0 : corruptedStats.size() * ENTRY_SIZE);
    }

    private static long estimatedSizeOf(Map<Integer, ?> values)
    {
        return values == null ? 0 : values.size() * ENTRY_SIZE;
    }

    public Map<Integer, Type.PrimitiveType> getIdToTypeMapping()
    {
        return idToTypeMapping;
    }

    public List<Types.NestedField> getNonPartitionPrimitiveColumns()
    {
        return nonPartitionPrimitiveColumns;
    }

    public StructLike getValues()
    {
        return values;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public long getFileCount()
    {
        return fileCount;
    }

    public long getSize()
    {
        return size;
    }

    public Map<Integer, Object> getMinValues()
    {
        return minValues;
    }

    public Map<Integer, Object> getMaxValues()
    {
        return maxValues;
    }

    public Map<Integer, Long> getNullCounts()
    {
        return nullCounts;
    }

    public Set<Integer> getCorruptedStats()
    {
        return corruptedStats;
    }

    public boolean hasValidColumnMetrics()
    {
        return hasValidColumnMetrics;
    }

    public void incrementRecordCount(long count)
    {
        this.recordCount += count;
    }

    public void incrementFileCount()
    {
        this.fileCount++;
    }

    public void incrementSize(long numberOfBytes)
    {
        this.size += numberOfBytes;
    }

    /**
     * The update logic is built with the following rules:
     * bounds is null => if any file has a missing bound for a column, that bound will not be reported
     * bounds is missing id => not reported in Parquet => that bound will not be reported
     * bound value is null => not an expected case
     * bound value is present => this is the normal case and bounds will be reported correctly
     */
    public void updateMin(Map<Integer, Object> lowerBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        updateStats(this.minValues, lowerBounds, nullCounts, recordCount, i -> (i > 0));
    }

    public void updateMax(Map<Integer, Object> upperBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        updateStats(this.maxValues, upperBounds, nullCounts, recordCount, i -> (i < 0));
    }

    public void updateStats(Map<Integer, Object> current, Map<Integer, Object> newStat, Map<Integer, Long> nullCounts, long recordCount, Predicate<Integer> predicate)
    {
        if (!hasValidColumnMetrics) {
            return;
        }
        if (newStat == null || nullCounts == null) {
            hasValidColumnMetrics = false;
            return;
        }
        for (Types.NestedField column : nonPartitionPrimitiveColumns) {
            int id = column.fieldId();

            if (corruptedStats.contains(id)) {
                continue;
            }

            Object newValue = newStat.get(id);
            // it is expected to not have min/max if all values are null for a column in the datafile and it is not a case of corrupted stats.
            if (newValue == null) {
                Long nullCount = nullCounts.get(id);
                if ((nullCount == null) || (nullCount != recordCount)) {
                    current.remove(id);
                    corruptedStats.add(id);
                }
                continue;
            }

            Object oldValue = current.putIfAbsent(id, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = Comparators.forType(idToTypeMapping.get(id));
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    current.put(id, newValue);
                }
            }
        }
    }

    public void updateNullCount(Map<Integer, Long> nullCounts)
    {
        if (!hasValidColumnMetrics) {
            return;
        }
        if (nullCounts == null) {
            hasValidColumnMetrics = false;
            return;
        }
        nullCounts.forEach((key, counts) ->
                this.nullCounts.merge(key, counts, Long::sum));
    }

    public static Map<Integer, Object> toMap(Map<Integer, Type.PrimitiveType> idToTypeMapping, Map<Integer, ByteBuffer> idToMetricMap)
    {
        if (idToMetricMap == null) {
            return null;
        }
        ImmutableMap.Builder<Integer, Object> map = ImmutableMap.builder();
        idToMetricMap.forEach((id, value) -> {
            Type.PrimitiveType type = idToTypeMapping.get(id);
            if (type == null) {
                // may occur for non-primitive types such as row-types
                return;
            }
            map.put(id, Conversions.fromByteBuffer(type, value));
        });
        return map.build();
    }
}
