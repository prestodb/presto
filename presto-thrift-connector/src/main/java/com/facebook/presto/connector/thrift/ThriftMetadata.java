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
package com.facebook.presto.connector.thrift;

import com.facebook.airlift.units.Duration;
import com.facebook.drift.TException;
import com.facebook.drift.client.DriftClient;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.thrift.annotations.ForMetadataRefresh;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.thrift.api.connector.PrestoThriftNullableSchemaName;
import com.facebook.presto.thrift.api.connector.PrestoThriftNullableTableMetadata;
import com.facebook.presto.thrift.api.connector.PrestoThriftSchemaTableName;
import com.facebook.presto.thrift.api.connector.PrestoThriftService;
import com.facebook.presto.thrift.api.connector.PrestoThriftServiceException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.facebook.presto.connector.thrift.ThriftErrorCode.THRIFT_SERVICE_INVALID_RESPONSE;
import static com.facebook.presto.connector.thrift.util.ThriftExceptions.toPrestoException;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;

public class ThriftMetadata
        implements ConnectorMetadata
{
    private static final Duration EXPIRE_AFTER_WRITE = new Duration(10, MINUTES);
    private static final Duration REFRESH_AFTER_WRITE = new Duration(2, MINUTES);

    private final DriftClient<PrestoThriftService> client;
    private final ThriftHeaderProvider thriftHeaderProvider;
    private final TypeManager typeManager;
    private final LoadingCache<SchemaTableName, Optional<ThriftTableMetadata>> tableCache;

    @Inject
    public ThriftMetadata(
            DriftClient<PrestoThriftService> client,
            ThriftHeaderProvider thriftHeaderProvider,
            TypeManager typeManager,
            @ForMetadataRefresh Executor metadataRefreshExecutor)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderProvider is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(EXPIRE_AFTER_WRITE.toMillis(), MILLISECONDS)
                .refreshAfterWrite(REFRESH_AFTER_WRITE.toMillis(), MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(this::getTableMetadataInternal), metadataRefreshExecutor));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try {
            return client.get(thriftHeaderProvider.getHeaders(session)).listSchemaNames();
        }
        catch (PrestoThriftServiceException | TException e) {
            throw toPrestoException(e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return tableCache.getUnchecked(tableName)
                .map(ThriftTableMetadata::getSchemaTableName)
                .map(ThriftTableHandle::new)
                .orElse(null);
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        ThriftTableHandle tableHandle = (ThriftTableHandle) table;
        ThriftTableLayoutHandle layoutHandle = new ThriftTableLayoutHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                desiredColumns,
                constraint.getSummary(),
                TRUE_CONSTANT);
        return new ConnectorTableLayoutResult(new ConnectorTableLayout(layoutHandle), constraint.getSummary());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ThriftTableHandle handle = ((ThriftTableHandle) tableHandle);
        return getRequiredTableMetadata(new SchemaTableName(handle.getSchemaName(), handle.getTableName())).toConnectorTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        try {
            return client.get(thriftHeaderProvider.getHeaders(session)).listTables(new PrestoThriftNullableSchemaName(schemaNameOrNull)).stream()
                    .map(PrestoThriftSchemaTableName::toSchemaTableName)
                    .collect(toImmutableList());
        }
        catch (PrestoThriftServiceException | TException e) {
            throw toPrestoException(e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(session, tableHandle).getColumns().stream().collect(toImmutableMap(ColumnMetadata::getName, ThriftColumnHandle::new));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ThriftColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return listTables(session, prefix.getSchemaName()).stream().collect(toImmutableMap(identity(), schemaTableName -> getRequiredTableMetadata(schemaTableName).getColumns()));
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        ThriftTableHandle table = (ThriftTableHandle) tableHandle;
        ThriftTableMetadata tableMetadata = getRequiredTableMetadata(new SchemaTableName(table.getSchemaName(), table.getTableName()));
        if (tableMetadata.containsIndexableColumns(indexableColumns)) {
            return Optional.of(new ConnectorResolvedIndex(new ThriftIndexHandle(tableMetadata.getSchemaTableName(), tupleDomain, session), tupleDomain));
        }
        else {
            return Optional.empty();
        }
    }

    private ThriftTableMetadata getRequiredTableMetadata(SchemaTableName schemaTableName)
    {
        Optional<ThriftTableMetadata> table = tableCache.getUnchecked(schemaTableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(schemaTableName);
        }
        else {
            return table.get();
        }
    }

    // this method makes actual thrift request and should be called only by cache load method
    private Optional<ThriftTableMetadata> getTableMetadataInternal(SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        PrestoThriftNullableTableMetadata thriftTableMetadata = getTableMetadata(schemaTableName);
        if (thriftTableMetadata.getTableMetadata() == null) {
            return Optional.empty();
        }
        ThriftTableMetadata tableMetadata = new ThriftTableMetadata(thriftTableMetadata.getTableMetadata(), typeManager);
        if (!Objects.equals(schemaTableName, tableMetadata.getSchemaTableName())) {
            throw new PrestoException(THRIFT_SERVICE_INVALID_RESPONSE, "Requested and actual table names are different");
        }
        return Optional.of(tableMetadata);
    }

    private PrestoThriftNullableTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        // treat invalid names as not found
        PrestoThriftSchemaTableName name;
        try {
            name = new PrestoThriftSchemaTableName(schemaTableName);
        }
        catch (IllegalArgumentException e) {
            return new PrestoThriftNullableTableMetadata(null);
        }

        try {
            return client.get().getTableMetadata(name);
        }
        catch (PrestoThriftServiceException | TException e) {
            throw toPrestoException(e);
        }
    }
}
