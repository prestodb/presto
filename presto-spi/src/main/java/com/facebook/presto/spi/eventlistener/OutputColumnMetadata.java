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
package com.facebook.presto.spi.eventlistener;

import com.facebook.presto.common.ColumnLineageEntry;
import com.facebook.presto.common.SourceColumn;
import com.facebook.presto.common.TransformationSubtype;
import com.facebook.presto.common.TransformationType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Per-output-column lineage metadata exposed to event listeners.
 *
 * <p>Internally, all lineage is stored as a single {@link Set} of
 * {@link ColumnLineageEntry}. {@link TransformationType#DIRECT} entries are
 * what older consumers know as "source columns" (the columns projected into
 * the output), and {@link TransformationType#INDIRECT} entries describe
 * columns that influenced the output without being projected (JOIN keys,
 * filter predicates, GROUP BY columns, etc).
 *
 * <p>For backward compatibility, {@link #getSourceColumns()} and
 * {@link #getIndirectSourceColumns()} are retained as derived views of the
 * unified set. New code should prefer {@link #getColumnLineage()} and
 * {@link #fromColumnLineage}.
 */
public class OutputColumnMetadata
{
    private final String columnName;
    private final String columnType;
    private final Set<ColumnLineageEntry> columnLineage;

    /**
     * @deprecated prefer {@link #fromColumnLineage(String, String, Set)}
     * which carries transformation metadata. This constructor records the
     * given source columns as {@code DIRECT/IDENTITY} entries.
     */
    @Deprecated
    public OutputColumnMetadata(String columnName, String columnType, Set<SourceColumn> sourceColumns)
    {
        this(columnName, columnType, sourceColumns, Collections.emptySet(), null);
    }

    /**
     * @deprecated prefer {@link #fromColumnLineage(String, String, Set)}
     * which avoids the split between {@code SourceColumn} (DIRECT) and
     * {@code ColumnLineageEntry} (INDIRECT). This constructor records the
     * given source columns as {@code DIRECT/IDENTITY} entries and merges
     * them with {@code indirectSourceColumns} into the unified store.
     */
    @Deprecated
    public OutputColumnMetadata(
            String columnName,
            String columnType,
            Set<SourceColumn> sourceColumns,
            Set<ColumnLineageEntry> indirectSourceColumns)
    {
        this(columnName, columnType, sourceColumns, indirectSourceColumns, null);
    }

    /**
     * @deprecated prefer {@link #fromColumnLineage(String, String, Set)}.
     * Retained as the JSON creator so that payloads from older producers,
     * which serialize {@code sourceColumns} and {@code indirectSourceColumns}
     * separately, continue to deserialize. Newer payloads carrying
     * {@code columnLineage} are also accepted; when present it takes
     * precedence over the split fields.
     */
    @Deprecated
    @JsonCreator
    public OutputColumnMetadata(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") String columnType,
            @JsonProperty("sourceColumns") Set<SourceColumn> sourceColumns,
            @JsonProperty("indirectSourceColumns") Set<ColumnLineageEntry> indirectSourceColumns,
            @JsonProperty("columnLineage") Set<ColumnLineageEntry> columnLineage)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.columnLineage = unifyLineage(sourceColumns, indirectSourceColumns, columnLineage);
    }

    /**
     * Build an {@code OutputColumnMetadata} from a unified column-lineage set
     * carrying both DIRECT and INDIRECT entries.
     */
    public static OutputColumnMetadata fromColumnLineage(String columnName, String columnType, Set<ColumnLineageEntry> columnLineage)
    {
        requireNonNull(columnLineage, "columnLineage is null");
        return new OutputColumnMetadata(columnName, columnType, null, null, columnLineage);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public String getColumnType()
    {
        return columnType;
    }

    /**
     * @deprecated use {@link #getColumnLineage()}. Returns only the
     * DIRECT entries from {@link #getColumnLineage()} projected back to
     * {@link SourceColumn}, dropping the transformation metadata.
     */
    @Deprecated
    @JsonProperty
    public Set<SourceColumn> getSourceColumns()
    {
        return columnLineage.stream()
                .filter(entry -> entry.getTransformationType() == TransformationType.DIRECT)
                .map(entry -> new SourceColumn(entry.getTableName(), entry.getColumnName()))
                .collect(toUnmodifiableLinkedSet());
    }

    /**
     * @deprecated use {@link #getColumnLineage()}. Returns only the
     * INDIRECT entries from {@link #getColumnLineage()}.
     */
    @Deprecated
    @JsonProperty
    public Set<ColumnLineageEntry> getIndirectSourceColumns()
    {
        return columnLineage.stream()
                .filter(entry -> entry.getTransformationType() == TransformationType.INDIRECT)
                .collect(toUnmodifiableLinkedSet());
    }

    private static <T> Collector<T, ?, Set<T>> toUnmodifiableLinkedSet()
    {
        return Collectors.collectingAndThen(
                Collectors.toCollection(LinkedHashSet::new),
                Collections::unmodifiableSet);
    }

    /**
     * Unified DIRECT + INDIRECT column-lineage entries for this output column.
     * Each entry carries the upstream table/column plus transformation type
     * and subtype.
     */
    @JsonProperty
    public Set<ColumnLineageEntry> getColumnLineage()
    {
        return columnLineage;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, columnLineage);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        OutputColumnMetadata other = (OutputColumnMetadata) obj;
        return Objects.equals(columnName, other.columnName) &&
                Objects.equals(columnType, other.columnType) &&
                Objects.equals(columnLineage, other.columnLineage);
    }

    private static Set<ColumnLineageEntry> unifyLineage(
            Set<SourceColumn> sourceColumns,
            Set<ColumnLineageEntry> indirectSourceColumns,
            Set<ColumnLineageEntry> columnLineage)
    {
        if (columnLineage != null && !columnLineage.isEmpty()) {
            return Collections.unmodifiableSet(new LinkedHashSet<>(columnLineage));
        }
        LinkedHashSet<ColumnLineageEntry> merged = new LinkedHashSet<>();
        if (sourceColumns != null) {
            for (SourceColumn source : sourceColumns) {
                merged.add(new ColumnLineageEntry(
                        source.getTableName(),
                        source.getColumnName(),
                        TransformationType.DIRECT,
                        TransformationSubtype.IDENTITY));
            }
        }
        if (indirectSourceColumns != null) {
            merged.addAll(indirectSourceColumns);
        }
        return Collections.unmodifiableSet(merged);
    }
}
