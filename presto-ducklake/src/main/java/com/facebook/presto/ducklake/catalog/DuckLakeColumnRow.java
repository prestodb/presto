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
package com.facebook.presto.ducklake.catalog;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A single, flat, visible-at-snapshot row of {@code ducklake_column}, including rows that
 * describe children nested inside a {@code list}/{@code struct}/{@code map} column via {@code
 * parent_column}. A later task ({@code DuckLakeColumnTree} in Task 2.1) assembles these flat rows
 * into a column tree; this class intentionally keeps no structure beyond the raw row.
 */
public class DuckLakeColumnRow
{
    private final long columnId;
    private final long columnOrder;
    private final String columnName;
    private final String columnType;
    private final Optional<String> initialDefault;
    private final boolean nullsAllowed;
    private final OptionalLong parentColumn;

    public DuckLakeColumnRow(
            long columnId,
            long columnOrder,
            String columnName,
            String columnType,
            Optional<String> initialDefault,
            boolean nullsAllowed,
            OptionalLong parentColumn)
    {
        this.columnId = columnId;
        this.columnOrder = columnOrder;
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.initialDefault = requireNonNull(initialDefault, "initialDefault is null");
        this.nullsAllowed = nullsAllowed;
        this.parentColumn = requireNonNull(parentColumn, "parentColumn is null");
    }

    public long getColumnId()
    {
        return columnId;
    }

    public long getColumnOrder()
    {
        return columnOrder;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getColumnType()
    {
        return columnType;
    }

    public Optional<String> getInitialDefault()
    {
        return initialDefault;
    }

    public boolean isNullsAllowed()
    {
        return nullsAllowed;
    }

    public OptionalLong getParentColumn()
    {
        return parentColumn;
    }
}
