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

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThriftService(value = "presto-catalog-server", idlName = "PrestoCatalogServer")
public class CatalogServer
{
    private static final String EMPTY_STRING = "";
    private final Metadata metadataProvider;
    private final SessionPropertyManager sessionPropertyManager;
    private final TransactionManager transactionManager;
    private final ObjectMapper objectMapper;
    private final LoadingCache<CacheKey, Boolean> catalogExistsCache;
    private final LoadingCache<CacheKey, Boolean> schemaExistsCache;
    private final LoadingCache<CacheKey, List<String>> listSchemaNamesCache;
    private final LoadingCache<CacheKey, Optional<TableHandle>> getTableHandleCache;
    private final LoadingCache<CacheKey, List<QualifiedObjectName>> listTablesCache;
    private final LoadingCache<CacheKey, List<QualifiedObjectName>> listViewsCache;
    private final LoadingCache<CacheKey, Map<QualifiedObjectName, ViewDefinition>> getViewsCache;
    private final LoadingCache<CacheKey, Optional<ViewDefinition>> getViewCache;
    private final LoadingCache<CacheKey, Optional<ConnectorMaterializedViewDefinition>> getMaterializedViewCache;
    private final LoadingCache<CacheKey, List<QualifiedObjectName>> getReferencedMaterializedViewsCache;

    @Inject
    public CatalogServer(MetadataManager metadataProvider, SessionPropertyManager sessionPropertyManager, TransactionManager transactionManager, ObjectMapper objectMapper)
    {
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.objectMapper = requireNonNull(objectMapper, "handleResolver is null");

        OptionalLong cacheExpiresAfterWriteMillis = OptionalLong.of(600000);
        long cacheMaximumSize = 1000;

        this.catalogExistsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadCatalogExists));
        this.schemaExistsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadSchemaExists));
        this.listSchemaNamesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadListSchemaNames));
        this.getTableHandleCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadGetTableHandle));
        this.listTablesCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadListTables));
        this.listViewsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadListViews));
        this.getViewsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadGetViews));
        this.getViewCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadGetView));
        this.getMaterializedViewCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadGetMaterializedView));
        this.getReferencedMaterializedViewsCache = newCacheBuilder(cacheExpiresAfterWriteMillis, cacheMaximumSize).build(CacheLoader.from(this::loadGetReferencedMaterializedViews));
    }

    /*
        Loading Cache Methods
     */

    private Boolean loadCatalogExists(CacheKey key)
    {
        return metadataProvider.catalogExists(key.getSession().toSession(sessionPropertyManager), (String) key.getKey());
    }

    private Boolean loadSchemaExists(CacheKey key)
    {
        return metadataProvider.schemaExists(key.getSession().toSession(sessionPropertyManager), (CatalogSchemaName) key.getKey());
    }

    private List<String> loadListSchemaNames(CacheKey key)
    {
        return metadataProvider.listSchemaNames(key.getSession().toSession(sessionPropertyManager), (String) key.getKey());
    }

    private Optional<TableHandle> loadGetTableHandle(CacheKey key)
    {
        return metadataProvider.getTableHandle(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    private List<QualifiedObjectName> loadListTables(CacheKey key)
    {
        return metadataProvider.listTables(key.getSession().toSession(sessionPropertyManager), (QualifiedTablePrefix) key.getKey());
    }

    private List<QualifiedObjectName> loadListViews(CacheKey key)
    {
        return metadataProvider.listViews(key.getSession().toSession(sessionPropertyManager), (QualifiedTablePrefix) key.getKey());
    }

    private Map<QualifiedObjectName, ViewDefinition> loadGetViews(CacheKey key)
    {
        return metadataProvider.getViews(key.getSession().toSession(sessionPropertyManager), (QualifiedTablePrefix) key.getKey());
    }

    private Optional<ViewDefinition> loadGetView(CacheKey key)
    {
        return metadataProvider.getView(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    private Optional<ConnectorMaterializedViewDefinition> loadGetMaterializedView(CacheKey key)
    {
        return metadataProvider.getMaterializedView(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    private List<QualifiedObjectName> loadGetReferencedMaterializedViews(CacheKey key)
    {
        return metadataProvider.getReferencedMaterializedViews(key.getSession().toSession(sessionPropertyManager), (QualifiedObjectName) key.getKey());
    }

    /*
        Metadata Manager Methods
     */

    @ThriftMethod
    public boolean schemaExists(TransactionInfo transactionInfo, SessionRepresentation session, CatalogSchemaName schema)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        return schemaExistsCache.getUnchecked(new CacheKey(session, schema));
    }

    @ThriftMethod
    public boolean catalogExists(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        return catalogExistsCache.getUnchecked(new CacheKey(session, catalogName));
    }

    @ThriftMethod
    public String listSchemaNames(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        List<String> schemaNames = listSchemaNamesCache.getUnchecked(new CacheKey(session, catalogName));
        if (!schemaNames.isEmpty()) {
            try {
                return objectMapper.writeValueAsString(schemaNames);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    @ThriftMethod
    public String getTableHandle(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName table)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, table);
        Optional<TableHandle> tableHandle = getTableHandleCache.getUnchecked(cacheKey);
        if (!tableHandle.isPresent()) {
            getTableHandleCache.refresh(cacheKey);
            tableHandle = getTableHandleCache.getUnchecked(cacheKey);
        }
        if (tableHandle.isPresent()) {
            try {
                return objectMapper.writeValueAsString(tableHandle.get());
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    @ThriftMethod
    public String listTables(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        List<QualifiedObjectName> tableList = listTablesCache.getUnchecked(new CacheKey(session, prefix));
        if (!tableList.isEmpty()) {
            try {
                return objectMapper.writeValueAsString(tableList);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    @ThriftMethod
    public String listViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        List<QualifiedObjectName> viewsList = listViewsCache.getUnchecked(new CacheKey(session, prefix));
        if (!viewsList.isEmpty()) {
            try {
                return objectMapper.writeValueAsString(viewsList);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    @ThriftMethod
    public String getViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        Map<QualifiedObjectName, ViewDefinition> viewsMap = getViewsCache.getUnchecked(new CacheKey(session, prefix));
        if (!viewsMap.isEmpty()) {
            try {
                return objectMapper.writeValueAsString(viewsMap);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    @ThriftMethod
    public String getView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        Optional<ViewDefinition> viewDefinition = getViewCache.getUnchecked(new CacheKey(session, viewName));
        if (viewDefinition.isPresent()) {
            try {
                return objectMapper.writeValueAsString(viewDefinition.get());
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    @ThriftMethod
    public String getMaterializedView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        Optional<ConnectorMaterializedViewDefinition> connectorMaterializedViewDefinition = getMaterializedViewCache.getUnchecked(new CacheKey(session, viewName));
        if (connectorMaterializedViewDefinition.isPresent()) {
            try {
                return objectMapper.writeValueAsString(connectorMaterializedViewDefinition.get());
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    @ThriftMethod
    public String getReferencedMaterializedViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName tableName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        List<QualifiedObjectName> referencedMaterializedViewsList = getReferencedMaterializedViewsCache.getUnchecked(new CacheKey(session, tableName));
        if (!referencedMaterializedViewsList.isEmpty()) {
            try {
                return objectMapper.writeValueAsString(referencedMaterializedViewsList);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return EMPTY_STRING;
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(OptionalLong expiresAfterWriteMillis, long maximumSize)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        return cacheBuilder.maximumSize(maximumSize).recordStats();
    }

    private static class CacheKey<T>
    {
        private final SessionRepresentation session;
        private final T key;

        private CacheKey(SessionRepresentation session, T key)
        {
            this.session = session;
            this.key = key;
        }

        public SessionRepresentation getSession()
        {
            return session;
        }

        public T getKey()
        {
            return key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(key, cacheKey.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key);
        }
    }
}
