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
package io.prestosql.spi.connector;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConnectorViewDefinition
{
    private final SchemaTableName name;
    private final Optional<String> owner;
    private final String viewData;

    public ConnectorViewDefinition(SchemaTableName name, Optional<String> owner, String viewData)
    {
        this.name = requireNonNull(name, "name is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.viewData = requireNonNull(viewData, "viewData is null");
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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorViewDefinition{");
        sb.append("name=").append(name);
        sb.append(", owner=").append(owner);
        sb.append('}');
        return sb.toString();
    }
}
