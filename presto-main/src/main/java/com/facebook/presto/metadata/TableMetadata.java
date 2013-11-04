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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Preconditions;

import java.util.List;

public class TableMetadata
{
    private final String connectorId;
    private final ConnectorTableMetadata metadata;

    public TableMetadata(String connectorId, ConnectorTableMetadata metadata)
    {
        Preconditions.checkNotNull(connectorId, "catalog is null");
        Preconditions.checkNotNull(metadata, "metadata is null");

        this.connectorId = connectorId;
        this.metadata = metadata;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    ConnectorTableMetadata getMetadata()
    {
        return metadata;
    }

    public SchemaTableName getTable()
    {
        return metadata.getTable();
    }

    public List<ColumnMetadata> getColumns()
    {
        return metadata.getColumns();
    }
}
