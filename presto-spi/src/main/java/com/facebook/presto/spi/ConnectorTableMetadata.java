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

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        if (table == null) {
            throw new NullPointerException("table is null or empty");
        }
        if (columns == null) {
            throw new NullPointerException("columns is null");
        }

        this.table = table;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("SchemaTableMetadata{");
        sb.append("table=").append(table);
        sb.append(", columns=").append(columns);
        sb.append('}');
        return sb.toString();
    }
}
