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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.connector.Connector;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Catalog
{
    private final String catalogName;
    private final ConnectorId connectorId;
    private final Connector connector;

    private final ConnectorId informationSchemaId;
    private final Connector informationSchema;

    private final ConnectorId systemTablesId;
    private final Connector systemTables;

    public Catalog(
            String catalogName,
            ConnectorId connectorId,
            Connector connector,
            ConnectorId informationSchemaId,
            Connector informationSchema,
            ConnectorId systemTablesId,
            Connector systemTables)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.informationSchemaId = requireNonNull(informationSchemaId, "informationSchemaId is null");
        this.informationSchema = requireNonNull(informationSchema, "informationSchema is null");
        this.systemTablesId = requireNonNull(systemTablesId, "systemTablesId is null");
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    public ConnectorId getInformationSchemaId()
    {
        return informationSchemaId;
    }

    public ConnectorId getSystemTablesId()
    {
        return systemTablesId;
    }

    public Connector getConnector(ConnectorId connectorId)
    {
        if (this.connectorId.equals(connectorId)) {
            return connector;
        }
        if (informationSchemaId.equals(connectorId)) {
            return informationSchema;
        }
        if (systemTablesId.equals(connectorId)) {
            return systemTables;
        }
        throw new IllegalArgumentException("Unknown connector id: " + connectorId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("connectorId", connectorId)
                .toString();
    }
}
