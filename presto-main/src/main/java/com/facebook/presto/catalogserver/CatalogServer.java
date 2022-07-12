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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.inject.Inject;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThriftService(value = "presto-catalog-server", idlName = "PrestoCatalogServer")
public class CatalogServer
{
    private static final String EMPTY_STRING = "";
    // TODO make cache constants configurable
    private static final Duration CACHE_EXPIRES_AFTER_WRITE_MILLIS = Duration.of(10, MINUTES);
    private static final long CACHE_MAXIMUM_SIZE = 1000;

    private final Metadata metadataProvider;
    private final SessionPropertyManager sessionPropertyManager;
    private final TransactionManager transactionManager;
    private final ObjectMapper objectMapper;
    private final CatalogServerCacheStats cacheStats;

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
        this.cacheStats = new CatalogServerCacheStats();

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .expireAfterWrite(CACHE_EXPIRES_AFTER_WRITE_MILLIS.toMillis(), MILLISECONDS);

        this.catalogExistsCache = cacheBuilder.build(CacheLoader.from(this::loadCatalogExists));
        this.schemaExistsCache = cacheBuilder.build(CacheLoader.from(this::loadSchemaExists));
        this.listSchemaNamesCache = cacheBuilder.build(CacheLoader.from(this::loadListSchemaNames));
        this.getTableHandleCache = cacheBuilder.build(CacheLoader.from(this::loadGetTableHandle));
        this.listTablesCache = cacheBuilder.build(CacheLoader.from(this::loadListTables));
        this.listViewsCache = cacheBuilder.build(CacheLoader.from(this::loadListViews));
        this.getViewsCache = cacheBuilder.build(CacheLoader.from(this::loadGetViews));
        this.getViewCache = cacheBuilder.build(CacheLoader.from(this::loadGetView));
        this.getMaterializedViewCache = cacheBuilder.build(CacheLoader.from(this::loadGetMaterializedView));
        this.getReferencedMaterializedViewsCache = cacheBuilder.build(CacheLoader.from(this::loadGetReferencedMaterializedViews));
    }

    /*
        Metadata Manager Methods
     */

    @ThriftMethod
    public boolean schemaExists(TransactionInfo transactionInfo, SessionRepresentation session, CatalogSchemaName schema)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey<>(session, schema);
        incrementCacheCount(cacheKey, schemaExistsCache, cacheStats.getSchemaExistsCacheStats());
        return get(schemaExistsCache, cacheKey);
    }

    @ThriftMethod
    public boolean catalogExists(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey<>(session, catalogName);
        incrementCacheCount(cacheKey, catalogExistsCache, cacheStats.getCatalogExistsCacheStats());
        return get(catalogExistsCache, cacheKey);
    }

    @ThriftMethod
    public String listSchemaNames(TransactionInfo transactionInfo, SessionRepresentation session, String catalogName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey<>(session, catalogName);
        incrementCacheCount(cacheKey, listSchemaNamesCache, cacheStats.getListSchemaNamesCacheStats());
        List<String> schemaNames = get(listSchemaNamesCache, cacheKey);
        return schemaNames.isEmpty()
                ? EMPTY_STRING
                : writeValueAsString(schemaNames, objectMapper);
    }

    @ThriftMethod
    public String getTableHandle(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName table)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, table);
        incrementCacheCount(cacheKey, getTableHandleCache, cacheStats.getGetTableHandleCacheStats());
        Optional<TableHandle> tableHandle = get(getTableHandleCache, cacheKey);
        if (!tableHandle.isPresent()) {
            getTableHandleCache.refresh(cacheKey);
            tableHandle = get(getTableHandleCache, cacheKey);
        }
        return tableHandle.map(handle -> writeValueAsString(handle, objectMapper))
                .orElse(EMPTY_STRING);
    }

    @ThriftMethod
    public String listTables(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, prefix);
        incrementCacheCount(cacheKey, listTablesCache, cacheStats.getListTablesCacheStats());
        List<QualifiedObjectName> tableList = get(listTablesCache, cacheKey);
        return tableList.isEmpty()
                ? EMPTY_STRING
                : writeValueAsString(tableList, objectMapper);
    }

    @ThriftMethod
    public String listViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, prefix);
        incrementCacheCount(cacheKey, listViewsCache, cacheStats.getListViewsCacheStats());
        List<QualifiedObjectName> viewsList = get(listViewsCache, cacheKey);
        return viewsList.isEmpty()
                ? EMPTY_STRING
                : writeValueAsString(viewsList, objectMapper);
    }

    @ThriftMethod
    public String getViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedTablePrefix prefix)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, prefix);
        incrementCacheCount(cacheKey, getViewsCache, cacheStats.getGetViewsCacheStats());
        Map<QualifiedObjectName, ViewDefinition> viewsMap = get(getViewsCache, cacheKey);
        return viewsMap.isEmpty()
                ? EMPTY_STRING
                : writeValueAsString(viewsMap, objectMapper);
    }

    @ThriftMethod
    public String getView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, viewName);
        incrementCacheCount(cacheKey, getViewCache, cacheStats.getGetViewCacheStats());
        Optional<ViewDefinition> viewDefinition = get(getViewCache, cacheKey);
        return viewDefinition.map(view -> writeValueAsString(view, objectMapper))
                .orElse(EMPTY_STRING);
    }

    @ThriftMethod
    public String getMaterializedView(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName viewName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, viewName);
        incrementCacheCount(cacheKey, getMaterializedViewCache, cacheStats.getGetMaterializedViewCacheStats());
        Optional<ConnectorMaterializedViewDefinition> connectorMaterializedViewDefinition = get(getMaterializedViewCache, cacheKey);
        return connectorMaterializedViewDefinition.map(materializedView -> writeValueAsString(materializedView, objectMapper))
                .orElse(EMPTY_STRING);
    }

    @ThriftMethod
    public String getReferencedMaterializedViews(TransactionInfo transactionInfo, SessionRepresentation session, QualifiedObjectName tableName)
    {
        transactionManager.tryRegisterTransaction(transactionInfo);
        CacheKey cacheKey = new CacheKey(session, tableName);
        incrementCacheCount(cacheKey, getReferencedMaterializedViewsCache, cacheStats.getGetReferencedMaterializedViewsCacheStats());
        List<QualifiedObjectName> referencedMaterializedViewsList = get(getReferencedMaterializedViewsCache, cacheKey);
        return referencedMaterializedViewsList.isEmpty()
                ? EMPTY_STRING
                : writeValueAsString(referencedMaterializedViewsList, objectMapper);
    }

    @ThriftMethod
    public CatalogServerCacheStats getCacheStats()
    {
        return cacheStats;
    }

    @ThriftMethod
    public void refreshCache()
    {
        catalogExistsCache.invalidateAll();
        schemaExistsCache.invalidateAll();
        listSchemaNamesCache.invalidateAll();
        getTableHandleCache.invalidateAll();
        listTablesCache.invalidateAll();
        listViewsCache.invalidateAll();
        getViewsCache.invalidateAll();
        getViewCache.invalidateAll();
        getMaterializedViewCache.invalidateAll();
        getReferencedMaterializedViewsCache.invalidateAll();
        cacheStats.clearAll();
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

    private String writeValueAsString(Object value, ObjectMapper objectMapper)
    {
        try {
            return objectMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private void incrementCacheCount(CacheKey cacheKey, LoadingCache loadingCache, CatalogServerCacheStats.CacheStats cacheStats)
    {
        if (loadingCache.getIfPresent(cacheKey) != null) {
            cacheStats.incrementCacheHit();
        }
        else {
            cacheStats.incrementCacheMiss();
        }
    }

    private static class CacheKey<T>
    {
        private final SessionRepresentation session;
        private final T key;

        private CacheKey(SessionRepresentation session, T key)
        {
            this.session = requireNonNull(session, "session is null");
            this.key = requireNonNull(key, "key is null");
        }

        public SessionRepresentation getSession()
        {
            return session;
        }

        public T getKey()
        {
            return key;
        }

        // Session changes across different queries. For caching to be effective across multiple queries,
        // we should NOT include session in equals() and hashCode() methods below.
        // Only the key value is to be considered in determining whether a cacheKey is unique or not
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
