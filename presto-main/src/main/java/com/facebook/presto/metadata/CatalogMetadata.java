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

import com.facebook.presto.Session;
import com.facebook.presto.connector.CatalogName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CatalogMetadata
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final CatalogName catalogName;
    private final ConnectorMetadata metadata;
    private final ConnectorTransactionHandle transactionHandle;

    private final CatalogName informationSchemaId;
    private final ConnectorMetadata informationSchema;
    private final ConnectorTransactionHandle informationSchemaTransactionHandle;

    private final CatalogName systemTablesId;
    private final ConnectorMetadata systemTables;
    private final ConnectorTransactionHandle systemTablesTransactionHandle;

    public CatalogMetadata(
            CatalogName catalogName,
            ConnectorMetadata metadata,
            ConnectorTransactionHandle transactionHandle,
            CatalogName informationSchemaId,
            ConnectorMetadata informationSchema,
            ConnectorTransactionHandle informationSchemaTransactionHandle,
            CatalogName systemTablesId,
            ConnectorMetadata systemTables,
            ConnectorTransactionHandle systemTablesTransactionHandle)
    {
        this.catalogName = requireNonNull(catalogName, "connectorId is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.informationSchemaId = requireNonNull(informationSchemaId, "informationSchemaId is null");
        this.informationSchema = requireNonNull(informationSchema, "informationSchema is null");
        this.informationSchemaTransactionHandle = requireNonNull(informationSchemaTransactionHandle, "informationSchemaTransactionHandle is null");
        this.systemTablesId = requireNonNull(systemTablesId, "systemTablesId is null");
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
        this.systemTablesTransactionHandle = requireNonNull(systemTablesTransactionHandle, "systemTablesTransactionHandle is null");
    }

    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    public ConnectorMetadata getMetadataFor(CatalogName catalogName)
    {
        if (catalogName.equals(this.catalogName)) {
            return metadata;
        }
        if (catalogName.equals(informationSchemaId)) {
            return informationSchema;
        }
        if (catalogName.equals(systemTablesId)) {
            return systemTables;
        }
        throw new IllegalArgumentException("Unknown connector id: " + catalogName);
    }

    public ConnectorTransactionHandle getTransactionHandleFor(CatalogName catalogName)
    {
        if (catalogName.equals(this.catalogName)) {
            return transactionHandle;
        }
        if (catalogName.equals(informationSchemaId)) {
            return informationSchemaTransactionHandle;
        }
        if (catalogName.equals(systemTablesId)) {
            return systemTablesTransactionHandle;
        }
        throw new IllegalArgumentException("Unknown connector id: " + catalogName);
    }

    public CatalogName getConnectorId(Session session, QualifiedObjectName table)
    {
        if (table.getSchemaName().equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemaId;
        }

        if (systemTables.getTableHandle(session.toConnectorSession(systemTablesId), table.asSchemaTableName()) != null) {
            return systemTablesId;
        }

        return catalogName;
    }

    public List<CatalogName> listConnectorIds()
    {
        return ImmutableList.of(informationSchemaId, systemTablesId, catalogName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", catalogName)
                .toString();
    }
}
