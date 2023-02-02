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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.sql.analyzer.MetadataResolver;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.ViewDefinition;
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

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
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
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        String schemaNamesJson = catalogServerClient.get().listSchemaNames(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                catalogName);
        return schemaNamesJson.isEmpty()
                ? ImmutableList.of()
                : readValue(schemaNamesJson, new TypeReference<List<String>>() {});
    }

    private Optional<TableHandle> getOptionalTableHandle(Session session, QualifiedObjectName table)
    {
        String tableHandleJson = catalogServerClient.get().getTableHandle(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                table);
        if (!tableHandleJson.isEmpty()) {
            TableHandle tableHandle = readValue(tableHandleJson, new TypeReference<TableHandle>() {});
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
        String tableListJson = catalogServerClient.get().listTables(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        return tableListJson.isEmpty()
                ? ImmutableList.of()
                : readValue(tableListJson, new TypeReference<List<QualifiedObjectName>>() {});
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        String viewsListJson = catalogServerClient.get().listViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        return viewsListJson.isEmpty()
                ? ImmutableList.of()
                : readValue(viewsListJson, new TypeReference<List<QualifiedObjectName>>() {});
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        String viewsMapJson = catalogServerClient.get().getViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                prefix);
        return viewsMapJson.isEmpty()
                ? ImmutableMap.of()
                : readValue(viewsMapJson, new TypeReference<Map<QualifiedObjectName, ViewDefinition>>() {});
    }

    @Override
    public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
    {
        String referencedMaterializedViewsListJson = catalogServerClient.get().getReferencedMaterializedViews(
                transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                session.toSessionRepresentation(),
                tableName);
        return referencedMaterializedViewsListJson.isEmpty()
                ? ImmutableList.of()
                : readValue(referencedMaterializedViewsListJson, new TypeReference<List<QualifiedObjectName>>() {});
    }

    @Override
    public MetadataResolver getMetadataResolver(Session session)
    {
        return new MetadataResolver()
        {
            @Override
            public boolean catalogExists(String catalogName)
            {
                return catalogServerClient.get().catalogExists(
                        transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                        session.toSessionRepresentation(),
                        catalogName);
            }

            @Override
            public boolean schemaExists(CatalogSchemaName schema)
            {
                return catalogServerClient.get().schemaExists(
                        transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                        session.toSessionRepresentation(),
                        schema);
            }

            @Override
            public boolean tableExists(QualifiedObjectName tableName)
            {
                return getTableHandle(tableName).isPresent();
            }

            @Override
            public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
            {
                return getOptionalTableHandle(session, tableName);
            }

            @Override
            public Optional<List<ColumnMetadata>> getColumns(QualifiedObjectName tableName)
            {
                Optional<TableHandle> tableHandle = getTableHandle(tableName);
                if (!tableHandle.isPresent()) {
                    throw new SemanticException(MISSING_TABLE, "Table does not exist: " + tableName.toString());
                }
                TableMetadata tableMetadata = getTableMetadata(session, tableHandle.get());
                return Optional.of(tableMetadata.getColumns());
            }

            @Override
            public Optional<ViewDefinition> getView(QualifiedObjectName viewName)
            {
                String viewDefinitionJson = catalogServerClient.get().getView(
                        transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                        session.toSessionRepresentation(),
                        viewName);
                return viewDefinitionJson.isEmpty()
                        ? Optional.empty()
                        : Optional.of(readValue(viewDefinitionJson, new TypeReference<ViewDefinition>() {}));
            }

            @Override
            public Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName)
            {
                String materializedViewDefinitionJson = catalogServerClient.get().getMaterializedView(
                        transactionManager.getTransactionInfo(session.getRequiredTransactionId()),
                        session.toSessionRepresentation(),
                        viewName);
                return materializedViewDefinitionJson.isEmpty()
                        ? Optional.empty()
                        : Optional.of(readValue(materializedViewDefinitionJson, new TypeReference<MaterializedViewDefinition>() {}));
            }
        };
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
