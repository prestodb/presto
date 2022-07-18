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
package com.facebook.presto.catalogserver;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.metadata.DelegatingMetadataManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

// TODO : Use thrift to serialize metadata objects instead of json serde on catalog server in the future
// TODO : Add e2e tests for this class
public class RemoteMetadataManager
        extends DelegatingMetadataManager
{
    private final TransactionManager transactionManager;
    private final ObjectMapper objectMapper;
    private final DriftClient<CatalogServerClient> catalogServerClient;

    @Inject
    public RemoteMetadataManager(
            MetadataManager metadataManager,
            TransactionManager transactionManager,
            ObjectMapper objectMapper,
            DriftClient<CatalogServerClient> catalogServerClient)
    {
        super(metadataManager);
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.catalogServerClient = requireNonNull(catalogServerClient, "catalogServerClient is null");
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        MetadataEntry<Boolean> metadataEntry = catalogServerClient.get().schemaExists(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                schema);
        return metadataEntry.getIsJson()
                ? false
                : metadataEntry.getValue();
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        MetadataEntry<Boolean> metadataEntry = catalogServerClient.get().catalogExists(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                catalogName);
        return metadataEntry.getIsJson()
                ? false
                : metadataEntry.getValue();
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().listSchemaNames(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                catalogName);
        return metadataEntry.getIsJson()
                ? readValue(metadataEntry.getValue(), new TypeReference<List<String>>() {})
                : ImmutableList.of();
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getTableHandle(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                table);
        if (metadataEntry.getIsJson()) {
            TableHandle tableHandle = readValue(metadataEntry.getValue(), new TypeReference<TableHandle>() {});
            Optional<CatalogMetadata> catalogMetadata = this.transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), table.getCatalogName());
            if (catalogMetadata.isPresent()) {
                tableHandle = new TableHandle(
                        tableHandle.getConnectorId(),
                        tableHandle.getConnectorHandle(),
                        catalogMetadata.get().getTransactionHandleFor(tableHandle.getConnectorId()),
                        tableHandle.getLayout());
            }
            return Optional.of(tableHandle);
        }
        return Optional.empty();
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().listTables(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        return metadataEntry.getIsJson()
                ? readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {})
                : ImmutableList.of();
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().listViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        return metadataEntry.getIsJson()
                ? readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {})
                : ImmutableList.of();
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        return metadataEntry.getIsJson()
                ? readValue(metadataEntry.getValue(), new TypeReference<Map<QualifiedObjectName, ViewDefinition>>() {})
                : ImmutableMap.of();
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getView(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                viewName);
        return metadataEntry.getIsJson()
                ? Optional.of(readValue(metadataEntry.getValue(), new TypeReference<ViewDefinition>() {}))
                : Optional.empty();
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getMaterializedView(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                viewName);
        return metadataEntry.getIsJson()
                ? Optional.of(readValue(metadataEntry.getValue(), new TypeReference<ConnectorMaterializedViewDefinition>() {}))
                : Optional.empty();
    }

    @Override
    public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
    {
        MetadataEntry<String> metadataEntry = catalogServerClient.get().getReferencedMaterializedViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                tableName);
        return metadataEntry.getIsJson()
                ? readValue(metadataEntry.getValue(), new TypeReference<List<QualifiedObjectName>>() {})
                : ImmutableList.of();
    }

    private <T> T readValue(String content, TypeReference<T> valueTypeRef)
    {
        try {
            return objectMapper.readValue(content, valueTypeRef);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
