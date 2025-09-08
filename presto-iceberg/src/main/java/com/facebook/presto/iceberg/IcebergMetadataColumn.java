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

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.MetadataColumns;

import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public enum IcebergMetadataColumn
{
    FILE_PATH(MetadataColumns.FILE_PATH.fieldId(), "$path", VARCHAR, PRIMITIVE),
    DATA_SEQUENCE_NUMBER(Integer.MAX_VALUE - 1001, "$data_sequence_number", BIGINT, PRIMITIVE),
    IS_DELETED(MetadataColumns.IS_DELETED.fieldId(), "$deleted", BOOLEAN, PRIMITIVE),
    DELETE_FILE_PATH(MetadataColumns.DELETE_FILE_PATH.fieldId(), "$delete_file_path", VARCHAR, PRIMITIVE),
    /**
     * Iceberg reserved row ids begin at INTEGER.MAX_VALUE and count down. Starting with MIN_VALUE here to avoid conflicts.
     * Inner type for row is not known until runtime.
     */
    UPDATE_ROW_DATA(Integer.MIN_VALUE, "$row_id", RowType.anonymous(ImmutableList.of(UNKNOWN)), STRUCT),
    MERGE_ROW_DATA(Integer.MIN_VALUE + 1, "$row_id", RowType.anonymous(ImmutableList.of(UNKNOWN)), STRUCT),
    MERGE_FILE_RECORD_COUNT(Integer.MIN_VALUE + 2, "file_record_count", BIGINT, PRIMITIVE),
    MERGE_PARTITION_SPEC_ID(Integer.MIN_VALUE + 3, "partition_spec_id", INTEGER, PRIMITIVE),
    MERGE_PARTITION_DATA(Integer.MIN_VALUE + 4, "partition_data", VARCHAR, PRIMITIVE)
    /**/;

    private static final Set<Integer> COLUMN_IDS = Stream.of(values())
            .map(IcebergMetadataColumn::getId)
            .collect(toImmutableSet());
    private final int id;
    private final String columnName;
    private final Type type;
    private final ColumnIdentity.TypeCategory typeCategory;

    IcebergMetadataColumn(int id, String columnName, Type type, ColumnIdentity.TypeCategory typeCategory)
    {
        this.id = id;
        this.columnName = columnName;
        this.type = type;
        this.typeCategory = typeCategory;
    }

    public int getId()
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

    public ColumnIdentity.TypeCategory getTypeCategory()
    {
        return typeCategory;
    }

    public static boolean isMetadataColumnId(int id)
    {
        return COLUMN_IDS.contains(id);
    }
}
