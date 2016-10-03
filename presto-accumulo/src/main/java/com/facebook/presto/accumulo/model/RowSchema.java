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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class RowSchema
{
    private final List<AccumuloColumnHandle> columns = new ArrayList<>();

    public RowSchema addRowId(String name, Type type)
    {
        columns.add(new AccumuloColumnHandle(name, Optional.empty(), Optional.empty(), type, columns.size(), "Accumulo row ID", false));
        return this;
    }

    public RowSchema addColumn(String prestoName, Optional<String> family, Optional<String> qualifier, Type type)
    {
        return addColumn(prestoName, family, qualifier, type, false);
    }

    public RowSchema addColumn(String prestoName, Optional<String> family, Optional<String> qualifier, Type type, boolean indexed)
    {
        columns.add(
                new AccumuloColumnHandle(
                        prestoName,
                        family,
                        qualifier,
                        type,
                        columns.size(),
                        format("Accumulo column %s:%s. Indexed: %b", family, qualifier, indexed),
                        indexed));
        return this;
    }

    public AccumuloColumnHandle getColumn(int i)
    {
        checkArgument(i >= 0 && i < columns.size(), "column index must be non-negative and less than length");
        return columns.get(i);
    }

    public AccumuloColumnHandle getColumn(String name)
    {
        for (AccumuloColumnHandle columnHandle : columns) {
            if (columnHandle.getName().equals(name)) {
                return columnHandle;
            }
        }

        throw new PrestoException(NOT_FOUND, "No column with name " + name);
    }

    public List<AccumuloColumnHandle> getColumns()
    {
        return columns;
    }

    public int getLength()
    {
        return columns.size();
    }

    /**
     * Creates a new {@link RowSchema} from a list of {@link AccumuloColumnHandle} objects. Does not validate the schema.
     *
     * @param columns Column handles
     * @return Row schema
     */
    public static RowSchema fromColumns(List<AccumuloColumnHandle> columns)
    {
        RowSchema schema = new RowSchema();
        for (AccumuloColumnHandle columnHandle : columns) {
            schema.addColumn(
                    columnHandle.getName(),
                    columnHandle.getFamily(),
                    columnHandle.getQualifier(),
                    columnHandle.getType(),
                    columnHandle.isIndexed());
        }
        return schema;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("{");
        for (AccumuloColumnHandle columnHandle : columns) {
            builder.append(columnHandle.getName())
                    .append(' ')
                    .append(columnHandle.getType())
                    .append(',');
        }

        if (builder.length() > 1) {
            builder.deleteCharAt(builder.length() - 1);
        }

        builder.append('}');
        return builder.toString();
    }
}
