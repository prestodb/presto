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

import static com.facebook.presto.accumulo.AccumuloErrorCode.COLUMN_NOT_FOUND;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Class to define the schema of a Row, stored as a list of {@link AccumuloColumnHandle}.
 */
public class RowSchema
{
    private List<AccumuloColumnHandle> columns = new ArrayList<>();

    /**
     * Gets a new instance of RowSchema.
     *
     * @return A new RowSchema
     */
    public static RowSchema newRowSchema()
    {
        return new RowSchema();
    }

    /**
     * Adds the Row ID column to the schema
     *
     * @param name Name of the row ID column
     * @param type Presto type of the column
     * @return this, for chaining
     */
    public RowSchema addRowId(String name, Type type)
    {
        columns.add(new AccumuloColumnHandle(name, Optional.empty(), Optional.empty(), type, columns.size(),
                "Accumulo row ID", false));
        return this;
    }

    /**
     * Appends a new non-indexed column to the end of the schema.
     *
     * @param prestoName Presto column name
     * @param family Accumulo column family, optional if column is row ID
     * @param qualifier Accumulo column qualifier, optional if column is row ID
     * @param type Presto type of the column
     * @return this, for schema
     */
    public RowSchema addColumn(String prestoName, Optional<String> family, Optional<String> qualifier, Type type)
    {
        return addColumn(prestoName, family, qualifier, type, false);
    }

    /**
     * Appends a new column to the end of the schema.
     *
     * @param prestoName Presto column name
     * @param family Accumulo column family, optional if column is row ID
     * @param qualifier Accumulo column qualifier, optional if column is row ID
     * @param type Presto type of the column
     * @param indexed True if indexed, false otherwise
     * @return this, for schema
     */
    public RowSchema addColumn(String prestoName, Optional<String> family, Optional<String> qualifier, Type type,
            boolean indexed)
    {
        columns.add(new AccumuloColumnHandle(prestoName, family, qualifier, type,
                columns.size(),
                format("Accumulo column %s:%s. Indexed: %b", family, qualifier, indexed),
                indexed));
        return this;
    }

    /**
     * Gets the column handle at the given zero-based indexed
     *
     * @param i Index to retrieve
     * @return The column handle
     */
    public AccumuloColumnHandle getColumn(int i)
    {
        checkArgument(i >= 0 && i < columns.size(), "column index must be non-negative and less than length");
        return columns.get(i);
    }

    /**
     * Gets the column handle that matches the given Presto column name
     *
     * @param name Presto column name
     * @return The column handle
     * @throws PrestoException If the column is not found
     */
    public AccumuloColumnHandle getColumn(String name)
    {
        for (AccumuloColumnHandle c : columns) {
            if (c.getName().equals(name)) {
                return c;
            }
        }

        throw new PrestoException(COLUMN_NOT_FOUND, format("No column with name %s", name));
    }

    /**
     * Gets all column handles in the schema
     *
     * @return Column handle
     */
    public List<AccumuloColumnHandle> getColumns()
    {
        return columns;
    }

    /**
     * Gets the length of the schema, i.e. number of fields
     *
     * @return Length of the schema
     */
    public int getLength()
    {
        return columns.size();
    }

    /**
     * Creates a new {@link RowSchema} from a list of {@link AccumuloColumnHandle} objects. Does not
     * validate the schema.
     *
     * @param cols Column handles
     * @return Row schema
     */
    public static RowSchema fromColumns(List<AccumuloColumnHandle> cols)
    {
        RowSchema schema = RowSchema.newRowSchema();
        for (AccumuloColumnHandle ach : cols) {
            schema.addColumn(ach.getName(), ach.getFamily(), ach.getQualifier(), ach.getType(),
                    ach.isIndexed());
        }
        return schema;
    }

    @Override
    public String toString()
    {
        StringBuilder bldr = new StringBuilder("{");
        for (AccumuloColumnHandle ach : columns) {
            bldr.append(ach.getName()).append(' ').append(ach.getType()).append(',');
        }

        if (bldr.length() > 1) {
            bldr.deleteCharAt(bldr.length() - 1);
        }

        bldr.append('}');
        return bldr.toString();
    }
}
