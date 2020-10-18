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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConnectorMaterializedViewDefinition
{
    private final SchemaTableName name;
    private final Optional<String> owner;
    private final String viewData;
    private final Optional<List<SchemaTableName>> baseTableNames;

    public ConnectorMaterializedViewDefinition(SchemaTableName name, Optional<String> owner, String viewData, Optional<List<SchemaTableName>> baseTableNames)
    {
        this.name = requireNonNull(name, "name is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.viewData = requireNonNull(viewData, "viewData is null");
        this.baseTableNames = requireNonNull(baseTableNames, "baseTableNames is null");
    }

    public SchemaTableName getName()
    {
        return name;
    }

    public Optional<String> getOwner()
    {
        return owner;
    }

    public String getViewData()
    {
        return viewData;
    }

    public Optional<List<SchemaTableName>> getBaseTableNames()
    {
        return baseTableNames;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorMaterializedViewDefinition{");
        sb.append("name=").append(name);
        sb.append(", owner=").append(owner);
        sb.append('}');
        return sb.toString();
    }
}
