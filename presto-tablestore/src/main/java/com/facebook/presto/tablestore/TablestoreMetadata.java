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
package com.facebook.presto.tablestore;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
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
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.tablestore.TypeUtil.checkType;
import static java.util.Objects.requireNonNull;

public class TablestoreMetadata
        implements ConnectorMetadata
{
    private final TablestoreFacade facade;

    @Inject
    public TablestoreMetadata(TablestoreFacade tablestoreFacade)
    {
        this.facade = requireNonNull(tablestoreFacade, "tablestoreFacade is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return facade.listSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        return facade.listTables(session, schemaNameOrNull);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName stn)
    {
        return facade.getTableHandle(session, stn);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        TablestoreTableHandle handle = checkType(table, TablestoreTableHandle.class);
        TupleDomain<ColumnHandle> tupleDomain = constraint.getSummary();
        TablestoreTableLayoutHandle tlh = new TablestoreTableLayoutHandle(handle, tupleDomain);
        ConnectorTableLayout tl = getTableLayout(session, tlh);
        return ImmutableList.of(new ConnectorTableLayoutResult(tl, tupleDomain));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        TablestoreTableLayoutHandle h = checkType(layoutHandle, TablestoreTableLayoutHandle.class);
        return new ConnectorTableLayout(h);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        TablestoreTableHandle oth = checkType(table, TablestoreTableHandle.class);

        List<ColumnMetadata> metadataList = oth.getOrderedColumnHandles().stream()
                .map(TablestoreColumnHandle::getColumnMetadata)
                .collect(Collectors.toList());

        return new TablestoreConnectorTableMetadata(oth, metadataList);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, TablestoreTableHandle.class).getColumnHandleMap();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle th, ColumnHandle ch)
    {
        return checkType(ch, TablestoreColumnHandle.class).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> list = listTables(session, prefix.getSchemaName());
        for (SchemaTableName stn : list) {
            String qtn = prefix.getTableName();
            if (qtn != null && !qtn.equalsIgnoreCase(stn.getTableName())) {
                continue;
            }

            TablestoreTableHandle th = facade.getTableHandle(session, stn);
            columns.put(stn, getTableMetadata(session, th).getColumns());
        }
        return columns.build();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TablestoreTableHandle h = TypeUtil.checkType(tableHandle, TablestoreTableHandle.class);

        return new TablestoreInsertTableHandle(h.getSchemaTableName(), h.getOrderedColumnHandles());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }
}
