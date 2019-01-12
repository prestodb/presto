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
package io.prestosql.plugin.localfile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalFileRecordSet
        implements RecordSet
{
    private final List<LocalFileColumnHandle> columns;
    private final List<Type> columnTypes;
    private final HostAddress address;
    private final TupleDomain<LocalFileColumnHandle> effectivePredicate;
    private final SchemaTableName tableName;
    private final LocalFileTables localFileTables;

    public LocalFileRecordSet(LocalFileTables localFileTables, LocalFileSplit split, List<LocalFileColumnHandle> columns)
    {
        this.columns = requireNonNull(columns, "column handles is null");
        requireNonNull(split, "split is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (LocalFileColumnHandle column : columns) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.address = Iterables.getOnlyElement(split.getAddresses());
        this.effectivePredicate = split.getEffectivePredicate();
        this.tableName = split.getTableName();

        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new LocalFileRecordCursor(localFileTables, columns, tableName, address, effectivePredicate);
    }
}
