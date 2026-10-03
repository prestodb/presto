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
package com.facebook.presto.ducklake.split.pruning;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The single entry point {@link com.facebook.presto.ducklake.split.DuckLakeSplitManager} and
 * {@link com.facebook.presto.ducklake.statistics.TableStatisticsMaker} both prune data files
 * through (spec &sect;4.5): first by partition value ({@link PartitionPruner}), then by per-file
 * column statistics ({@link StatisticsPruner}). Sharing this one method keeps the row counts the
 * statistics maker reports for a constrained table in exact agreement with the files the split
 * source actually reads for the same constraint.
 */
public final class DataFilePruner
{
    private DataFilePruner() {}

    public static List<DuckLakeDataFile> prune(
            List<DuckLakeDataFile> files,
            List<DuckLakePartitionField> partitionFields,
            TupleDomain<ColumnHandle> predicate)
    {
        requireNonNull(files, "files is null");
        requireNonNull(partitionFields, "partitionFields is null");
        requireNonNull(predicate, "predicate is null");

        Map<Long, DuckLakeColumnHandle> columnsById = columnsById(predicate);
        List<DuckLakeDataFile> partitionPruned = PartitionPruner.prune(files, partitionFields, columnsById, predicate);
        return StatisticsPruner.prune(partitionPruned, columnsById, predicate);
    }

    /**
     * The predicate's own domain columns, keyed by column id: every column with a domain in a
     * DuckLake {@code TupleDomain<ColumnHandle>} is necessarily a {@link DuckLakeColumnHandle}.
     */
    private static Map<Long, DuckLakeColumnHandle> columnsById(TupleDomain<ColumnHandle> predicate)
    {
        ImmutableMap.Builder<Long, DuckLakeColumnHandle> columnsById = ImmutableMap.builder();
        predicate.getDomains().ifPresent(domains -> domains.keySet().forEach(handle -> {
            DuckLakeColumnHandle column = (DuckLakeColumnHandle) handle;
            columnsById.put(column.getId(), column);
        }));
        return columnsById.build();
    }
}
