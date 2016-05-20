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

package com.facebook.presto.spi.eventlistener;

import java.util.List;

public class QueryInputMetadata
{
    private final String connectorId;
    private final String schema;
    private final String table;
    private final List<String> columns;

    public QueryInputMetadata(String connectorId, String schema, String table, List<String> columns)
    {
        this.connectorId = connectorId;
        this.schema = schema;
        this.table = table;
        this.columns = columns;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public List<String> getColumns()
    {
        return columns;
    }
}
