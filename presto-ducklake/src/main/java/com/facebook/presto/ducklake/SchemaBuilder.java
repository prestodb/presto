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
package com.facebook.presto.ducklake;

import com.facebook.presto.ducklake.catalog.DuckLakeColumnRow;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

/**
 * Assembles the flat {@code ducklake_column} rows of a table visible at a snapshot (see {@link
 * com.facebook.presto.ducklake.catalog.DuckLakeCatalog#listColumns}) into {@link
 * DuckLakeColumnIdentity} trees, and packages those trees together with the table's partition
 * fields into a {@link PrestoDuckLakeSchema}.
 */
public final class SchemaBuilder
{
    private SchemaBuilder() {}

    /**
     * Groups {@code rows} by {@code parent_column} and orders each group by {@code column_order}
     * (rows are not assumed to already be in that order), building the top-level column trees.
     *
     * @throws PrestoException with {@link DuckLakeErrorCode#DUCKLAKE_INVALID_METADATA} if a row's
     *     {@code parent_column} does not identify another row, or if the rows contain a cycle
     */
    public static List<DuckLakeColumnIdentity> buildColumns(List<DuckLakeColumnRow> rows)
    {
        requireNonNull(rows, "rows is null");

        Map<Long, DuckLakeColumnRow> rowsById = rows.stream()
                .collect(Collectors.toMap(DuckLakeColumnRow::getColumnId, row -> row, (a, b) -> a, LinkedHashMap::new));

        for (DuckLakeColumnRow row : rows) {
            OptionalLong parent = row.getParentColumn();
            if (parent.isPresent() && !rowsById.containsKey(parent.getAsLong())) {
                throw new PrestoException(DUCKLAKE_INVALID_METADATA, format(
                        "Column '%s' (id %s) references missing parent column id %s",
                        row.getColumnName(), row.getColumnId(), parent.getAsLong()));
            }
        }

        List<DuckLakeColumnRow> sortedRows = rows.stream()
                .sorted(comparingLong(DuckLakeColumnRow::getColumnOrder))
                .collect(toImmutableList());
        Map<OptionalLong, List<DuckLakeColumnRow>> childrenByParent = new LinkedHashMap<>();
        for (DuckLakeColumnRow row : sortedRows) {
            childrenByParent.computeIfAbsent(row.getParentColumn(), key -> new ArrayList<>()).add(row);
        }

        Set<Long> visited = new HashSet<>();
        List<DuckLakeColumnIdentity> topLevel = childrenByParent.getOrDefault(OptionalLong.empty(), ImmutableList.of()).stream()
                .map(row -> buildColumn(row, childrenByParent, visited, new LinkedHashSet<>()))
                .collect(toImmutableList());

        if (visited.size() != rows.size()) {
            Set<Long> unreached = rows.stream()
                    .map(DuckLakeColumnRow::getColumnId)
                    .filter(id -> !visited.contains(id))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            throw new PrestoException(DUCKLAKE_INVALID_METADATA, format(
                    "Column hierarchy contains a cycle involving column id(s) %s", unreached));
        }

        return topLevel;
    }

    /**
     * Builds a {@link PrestoDuckLakeSchema} from a table's column rows and partition fields.
     * {@code initial_default} values on nested (non-top-level) columns are ignored: only top-level
     * columns' defaults are meaningful to Presto.
     */
    public static PrestoDuckLakeSchema buildSchema(List<DuckLakeColumnRow> rows, List<DuckLakePartitionField> partitionFields)
    {
        requireNonNull(rows, "rows is null");
        requireNonNull(partitionFields, "partitionFields is null");

        List<DuckLakeColumnIdentity> columns = buildColumns(rows);

        ImmutableMap.Builder<Long, String> initialDefaults = ImmutableMap.builder();
        for (DuckLakeColumnRow row : rows) {
            if (row.getParentColumn().isPresent()) {
                continue;
            }
            row.getInitialDefault().ifPresent(value -> initialDefaults.put(row.getColumnId(), value));
        }

        return new PrestoDuckLakeSchema(columns, initialDefaults.build(), partitionFields);
    }

    private static DuckLakeColumnIdentity buildColumn(
            DuckLakeColumnRow row,
            Map<OptionalLong, List<DuckLakeColumnRow>> childrenByParent,
            Set<Long> visited,
            Set<Long> ancestors)
    {
        if (!ancestors.add(row.getColumnId())) {
            throw new PrestoException(DUCKLAKE_INVALID_METADATA, format(
                    "Column hierarchy contains a cycle at column id %s", row.getColumnId()));
        }
        visited.add(row.getColumnId());

        List<DuckLakeColumnIdentity> children = childrenByParent.getOrDefault(OptionalLong.of(row.getColumnId()), ImmutableList.of())
                .stream()
                .map(child -> buildColumn(child, childrenByParent, visited, ancestors))
                .collect(toImmutableList());

        ancestors.remove(row.getColumnId());

        return new DuckLakeColumnIdentity(row.getColumnId(), row.getColumnName(), typeCategoryOf(row.getColumnType()), row.getColumnType(), children);
    }

    private static DuckLakeColumnIdentity.TypeCategory typeCategoryOf(String duckLakeType)
    {
        switch (duckLakeType.trim().toLowerCase(Locale.ENGLISH)) {
            case "list":
                return DuckLakeColumnIdentity.TypeCategory.ARRAY;
            case "struct":
                return DuckLakeColumnIdentity.TypeCategory.STRUCT;
            case "map":
                return DuckLakeColumnIdentity.TypeCategory.MAP;
            default:
                return DuckLakeColumnIdentity.TypeCategory.PRIMITIVE;
        }
    }
}
