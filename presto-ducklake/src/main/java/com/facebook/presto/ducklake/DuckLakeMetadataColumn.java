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

import com.facebook.presto.common.type.Type;

import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.PRIMITIVE;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * The hidden, synthetic columns {@link DuckLakeColumnHandle} exposes on every table, modeled on
 * Iceberg's {@code IcebergMetadataColumn}. Ids are assigned from the top of the {@code long}
 * range, counting down, so they never collide with a real DuckLake {@code column_id} (which
 * DuckLake assigns starting from 0).
 */
public enum DuckLakeMetadataColumn
{
    PATH(Long.MAX_VALUE - 1, "$path", VARCHAR, PRIMITIVE),
    ROW_ID(Long.MAX_VALUE - 2, "$row_id", BIGINT, PRIMITIVE),
    /**
     * The physical row position of a row inside one Parquet data file. Never listed as a table
     * column; used internally by the delete filter and by row-id computation (Phase 4).
     */
    ROW_POSITION(Long.MAX_VALUE - 3, "$row_position", BIGINT, PRIMITIVE);

    private static final Set<Long> COLUMN_IDS = Stream.of(values())
            .map(DuckLakeMetadataColumn::getId)
            .collect(toImmutableSet());

    private final long id;
    private final String columnName;
    private final Type type;
    private final DuckLakeColumnIdentity.TypeCategory typeCategory;

    DuckLakeMetadataColumn(long id, String columnName, Type type, DuckLakeColumnIdentity.TypeCategory typeCategory)
    {
        this.id = id;
        this.columnName = columnName;
        this.type = type;
        this.typeCategory = typeCategory;
    }

    public long getId()
    {
        return id;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getType()
    {
        return type;
    }

    public DuckLakeColumnIdentity.TypeCategory getTypeCategory()
    {
        return typeCategory;
    }

    public static boolean isMetadataColumnId(long id)
    {
        return COLUMN_IDS.contains(id);
    }
}
