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
package com.facebook.presto.iceberg.changelog;

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.TypeConverter;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;

import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ChangelogUtil
{
    private ChangelogUtil() {}

    @FunctionalInterface
    protected interface Function2<T1, T2, R>
    {
        R apply(T1 arg1, T2 arg2);
    }

    public static Schema changelogTableSchema(RowType rowType)
    {
        return new Schema(
                Types.NestedField.required(1, "operation", Types.StringType.get()),
                Types.NestedField.required(2, "ordinal", Types.LongType.get()),
                Types.NestedField.required(3, "snapshotid", Types.LongType.get()),
                Types.NestedField.required(4, "rowdata", TypeConverter.fromRow(rowType, 4)));
    }

    public static ConnectorTableMetadata getChangelogTableMeta(SchemaTableName tableName, TypeManager typeManager, List<ColumnMetadata> columns)
    {
        return new ConnectorTableMetadata(tableName, getColumnMetadata(typeManager, columns));
    }

    public static RowType getRowTypeFromColumnMeta(List<ColumnMetadata> columns)
    {
        return RowType.from(columns.stream()
                .map(column -> RowType.field(column.getName(), column.getType()))
                .collect(toImmutableList()));
    }

    public static List<ColumnMetadata> getColumnMetadata(TypeManager typeManager, List<ColumnMetadata> columns)
    {
        return changelogTableSchema(getRowTypeFromColumnMeta(columns)).columns().stream()
                .map(column -> new ColumnMetadata(column.name(), toPrestoType(column.type(), typeManager)))
                .collect(toImmutableList());
    }
}
