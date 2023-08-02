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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.TypeConverter;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;

public class ChangelogUtil
{
    private ChangelogUtil() {}

    @FunctionalInterface
    protected interface Function2<T1, T2, R>
    {
        R apply(T1 arg1, T2 arg2);
    }

    public static Schema changelogTableSchema(Type primaryKeyType, RowType rowType)
    {
        return new Schema(
                Types.NestedField.required(1, "operation", Types.StringType.get()),
                Types.NestedField.required(2, "ordinal", Types.LongType.get()),
                Types.NestedField.required(3, "snapshotid", Types.LongType.get()),
                Types.NestedField.required(4, "primarykey", primaryKeyType),
                Types.NestedField.required(5, "rowdata", TypeConverter.fromRow(rowType, 5)));
    }

    public static ConnectorTableMetadata getChangelogTableMeta(SchemaTableName tableName, Type primaryKeyType, TypeManager typeManager, List<ColumnMetadata> columns)
    {
        return new ConnectorTableMetadata(tableName, getColumnMetadata(typeManager, primaryKeyType, columns));
    }

    public static Type getPrimaryKeyType(Table table, String columnName)
    {
        return table.schema().findType(columnName);
    }

    public static Type getPrimaryKeyType(ConnectorTableMetadata table, String columnName)
    {
        return toIcebergType(table.getColumns().stream().filter(x -> x.getName().equals(columnName)).findFirst().get().getType());
    }

    public static RowType getRowTypeFromSchema(Schema schema, TypeManager typeManager)
    {
        return RowType.from(IcebergUtil.getColumns(schema, typeManager).stream().map(col -> RowType.field(col.getName(), col.getType())).collect(Collectors.toList()));
    }

    public static RowType getRowTypeFromColumnMeta(List<ColumnMetadata> columns)
    {
        return RowType.from(columns.stream().map(col -> RowType.field(col.getName(), col.getType())).collect(Collectors.toList()));
    }

    public static List<ColumnMetadata> getColumnMetadata(TypeManager typeManager, Type primaryKeyType, List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        changelogTableSchema(primaryKeyType, getRowTypeFromColumnMeta(columns)).columns().stream()
                .map(x -> new ColumnMetadata(x.name(), toPrestoType(x.type(), typeManager)))
                .forEach(builder::add);
        return builder.build();
    }

    /**
     * Converts a tuple domain with the column name "primary_key" to the same domain with a column
     * handle corresponding to the primary key's actual column name and id.
     * <br>
     * If any additional filters exist which aren't columns on the actual iceberg table then they
     * are removed from the {@link TupleDomain}
     *
     * @return A {@link TupleDomain} which is valid for the actual iceberg table rather than the
     * changelog version of the table.
     */
    public static TupleDomain<IcebergColumnHandle> convertTupleDomain(TupleDomain<IcebergColumnHandle> tupleDomain, String primaryKeyColumnName, Table actualTable, TypeManager typeManager)
    {
        IcebergColumnHandle primaryKeyColumn = getColumns(actualTable.schema(), typeManager).stream()
                .filter(x -> x.getName().equalsIgnoreCase(primaryKeyColumnName)).findFirst().get();
        return tupleDomain.getDomains().map(x -> {
            ImmutableList.Builder<TupleDomain.ColumnDomain<IcebergColumnHandle>> newDomains = ImmutableList.builder();
            for (Map.Entry<IcebergColumnHandle, Domain> handle : x.entrySet()) {
                if (handle.getKey().getName().equalsIgnoreCase(ChangelogPageSource.ChangelogSchemaColumns.PRIMARYKEY.name())) {
                    newDomains.add(new TupleDomain.ColumnDomain<>(primaryKeyColumn, handle.getValue()));
                    break; // there should only be one column
                }
                else {
                    return TupleDomain.<IcebergColumnHandle>all();
                }
            }
            return TupleDomain.fromColumnDomains(Optional.of(newDomains.build()));
        }).orElseGet(TupleDomain::all);
    }
}
