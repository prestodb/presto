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
package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConnectorTableMetadata
{
    private final SchemaTableName table;
    private final List<ColumnMetadata> columns;
    /* nullable */
    private final String owner;

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        this(table, columns, null);
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, String owner)
    {
        if (table == null) {
            throw new NullPointerException("table is null or empty");
        }
        if (columns == null) {
            throw new NullPointerException("columns is null");
        }

        this.table = table;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.owner = owner;
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    /**
     * @return table owner or null
     */
    public String getOwner()
    {
        return owner;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorTableMetadata{");
        sb.append("table=").append(table);
        sb.append(", columns=").append(columns);
        sb.append(", owner=").append(owner);
        sb.append('}');
        return sb.toString();
    }
}
