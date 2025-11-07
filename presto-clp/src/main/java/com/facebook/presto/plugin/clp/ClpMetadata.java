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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * For efficiency, this class maintains two caches for metadata from the
 * {@link ClpMetadataProvider}:
 * <ul>
 *     <li>columnHandleCache: Maps {@link SchemaTableName} to a list of {@link ClpColumnHandle}s.
 *     </li>
 *     <li>tableHandleCache: Maps schema names to a list of {@link ClpTableHandle}s.</li>
 * </ul>
 */
public class ClpMetadata
        implements ConnectorMetadata
{
    public static final String DEFAULT_SCHEMA_NAME = "default";

    private final ClpMetadataProvider clpMetadataProvider;
    private final LoadingCache<SchemaTableName, List<ClpColumnHandle>> columnHandleCache;
    private final LoadingCache<String, List<ClpTableHandle>> tableHandleCache;

    @Inject
    public ClpMetadata(ClpConfig clpConfig, ClpMetadataProvider clpMetadataProvider)
    {
        this.columnHandleCache = CacheBuilder.newBuilder()
                .expireAfterWrite(clpConfig.getMetadataExpireInterval(), SECONDS)
                .refreshAfterWrite(clpConfig.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadColumnHandles));
        this.tableHandleCache = CacheBuilder.newBuilder()
                .expireAfterWrite(clpConfig.getMetadataExpireInterval(), SECONDS)
                .refreshAfterWrite(clpConfig.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadTableHandles));

        this.clpMetadataProvider = requireNonNull(clpMetadataProvider, "ClpMetadataProvider is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return clpMetadataProvider.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schemaNameValue = schemaName.orElse(DEFAULT_SCHEMA_NAME);
        if (!listSchemaNames(session).contains(schemaNameValue)) {
            return ImmutableList.of();
        }

        return listTables(schemaNameValue).stream()
                .map(tableHandle -> new SchemaTableName(schemaNameValue, tableHandle.getSchemaTableName().getTableName()))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        String schemaName = tableName.getSchemaName();
        if (!listSchemaNames(session).contains(schemaName)) {
            return null;
        }

        return listTables(schemaName).stream()
                .filter(tableHandle -> tableHandle.getSchemaTableName().equals(tableName))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        ClpTableHandle tableHandle = (ClpTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ClpTableLayoutHandle(tableHandle, Optional.empty(), Optional.empty()));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) table;
        SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
        List<ColumnMetadata> columns = listColumns(schemaTableName).stream()
                .map(ClpColumnHandle::getColumnMetadata)
                .collect(toImmutableList());

        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        String schemaName = prefix.getSchemaName();
        if (schemaName != null && !listSchemaNames(session).contains(schemaName)) {
            return ImmutableMap.of();
        }

        List<SchemaTableName> schemaTableNames;
        if (prefix.getTableName() == null) {
            schemaTableNames = listTables(session, Optional.ofNullable(prefix.getSchemaName()));
        }
        else {
            SchemaTableName table = new SchemaTableName(schemaName, prefix.getTableName());
            if (listTables(session, Optional.ofNullable(schemaName)).contains(table)) {
                schemaTableNames = ImmutableList.of(table);
            }
            else {
                schemaTableNames = ImmutableList.of();
            }
        }

        return schemaTableNames.stream()
                .collect(toImmutableMap(
                        Function.identity(),
                        tableName -> getTableMetadata(session, getTableHandle(session, tableName)).getColumns()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle;
        return listColumns(clpTableHandle.getSchemaTableName()).stream().collect(toImmutableMap(ClpColumnHandle::getColumnName, column -> column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ClpColumnHandle clpColumnHandle = (ClpColumnHandle) columnHandle;
        return clpColumnHandle.getColumnMetadata();
    }

    private List<ClpColumnHandle> loadColumnHandles(SchemaTableName schemaTableName)
    {
        return clpMetadataProvider.listColumnHandles(schemaTableName);
    }

    private List<ClpTableHandle> loadTableHandles(String schemaName)
    {
        return clpMetadataProvider.listTableHandles(schemaName);
    }

    private List<ClpColumnHandle> listColumns(SchemaTableName schemaTableName)
    {
        return columnHandleCache.getUnchecked(schemaTableName);
    }

    private List<ClpTableHandle> listTables(String schemaName)
    {
        return tableHandleCache.getUnchecked(schemaName);
    }
}
