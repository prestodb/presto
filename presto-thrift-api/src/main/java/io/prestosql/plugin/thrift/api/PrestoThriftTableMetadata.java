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
package io.prestosql.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftTableMetadata
{
    private final PrestoThriftSchemaTableName schemaTableName;
    private final List<PrestoThriftColumnMetadata> columns;
    private final String comment;
    private final List<Set<String>> indexableKeys;

    @ThriftConstructor
    public PrestoThriftTableMetadata(
            @ThriftField(name = "schemaTableName") PrestoThriftSchemaTableName schemaTableName,
            @ThriftField(name = "columns") List<PrestoThriftColumnMetadata> columns,
            @ThriftField(name = "comment") @Nullable String comment,
            @ThriftField(name = "indexableKeys") @Nullable List<Set<String>> indexableKeys)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.comment = comment;
        this.indexableKeys = indexableKeys;
    }

    @ThriftField(1)
    public PrestoThriftSchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @ThriftField(2)
    public List<PrestoThriftColumnMetadata> getColumns()
    {
        return columns;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public String getComment()
    {
        return comment;
    }

    /**
     * Returns a list of key sets which can be used for index lookups.
     * The list is expected to have only unique key sets.
     * {@code set<set<string>>} is not used here because some languages (like php) don't support it.
     */
    @Nullable
    @ThriftField(value = 4, requiredness = OPTIONAL)
    public List<Set<String>> getIndexableKeys()
    {
        return indexableKeys;
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
        PrestoThriftTableMetadata other = (PrestoThriftTableMetadata) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.columns, other.columns) &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, columns, comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("numberOfColumns", columns.size())
                .add("comment", comment)
                .toString();
    }
}
