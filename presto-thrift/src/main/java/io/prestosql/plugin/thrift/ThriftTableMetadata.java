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
package io.prestosql.plugin.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.thrift.api.PrestoThriftColumnMetadata;
import io.prestosql.plugin.thrift.api.PrestoThriftTableMetadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

class ThriftTableMetadata
{
    private final SchemaTableName schemaTableName;
    private final List<ColumnMetadata> columns;
    private final Optional<String> comment;
    private final Set<Set<String>> indexableKeys;

    public ThriftTableMetadata(SchemaTableName schemaTableName, List<ColumnMetadata> columns, Optional<String> comment, List<Set<String>> indexableKeys)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.comment = requireNonNull(comment, "comment is null");
        this.indexableKeys = deepImmutableCopy(requireNonNull(indexableKeys, "indexableKeys is null"));
    }

    public ThriftTableMetadata(PrestoThriftTableMetadata thriftTableMetadata, TypeManager typeManager)
    {
        this(thriftTableMetadata.getSchemaTableName().toSchemaTableName(),
                columnMetadata(thriftTableMetadata.getColumns(), typeManager),
                Optional.ofNullable(thriftTableMetadata.getComment()),
                thriftTableMetadata.getIndexableKeys() != null ? thriftTableMetadata.getIndexableKeys() : ImmutableList.of());
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public boolean containsIndexableColumns(Set<ColumnHandle> indexableColumns)
    {
        Set<String> keyColumns = indexableColumns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableSet());
        return indexableKeys.contains(keyColumns);
    }

    public ConnectorTableMetadata toConnectorTableMetadata()
    {
        return new ConnectorTableMetadata(
                schemaTableName,
                columns,
                ImmutableMap.of(),
                comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ThriftTableMetadata other = (ThriftTableMetadata) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.columns, other.columns) &&
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.indexableKeys, other.indexableKeys);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, columns, comment, indexableKeys);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("columns", columns)
                .add("comment", comment)
                .add("indexableKeys", indexableKeys)
                .toString();
    }

    private static List<ColumnMetadata> columnMetadata(List<PrestoThriftColumnMetadata> columns, TypeManager typeManager)
    {
        return columns.stream()
                .map(column -> column.toColumnMetadata(typeManager))
                .collect(toImmutableList());
    }

    private static Set<Set<String>> deepImmutableCopy(List<Set<String>> indexableKeys)
    {
        return indexableKeys.stream().map(ImmutableSet::copyOf).collect(toImmutableSet());
    }
}
