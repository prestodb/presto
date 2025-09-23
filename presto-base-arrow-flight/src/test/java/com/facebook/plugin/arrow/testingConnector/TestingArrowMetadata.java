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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.plugin.arrow.ArrowColumnHandle;
import com.facebook.plugin.arrow.ArrowMetadata;
import com.facebook.plugin.arrow.BaseArrowFlightClientHandler;
import com.facebook.plugin.arrow.testingConnector.tvf.QueryFunctionProvider;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TestingArrowMetadata
        extends ArrowMetadata
{
    @Inject
    public TestingArrowMetadata(BaseArrowFlightClientHandler clientHandler, ArrowBlockBuilder arrowBlockBuilder)
    {
        super(clientHandler, arrowBlockBuilder);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof QueryFunctionProvider.QueryFunctionHandle) {
            QueryFunctionProvider.QueryFunctionHandle functionHandle = (QueryFunctionProvider.QueryFunctionHandle) handle;
            return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), new ArrayList<>(functionHandle.getTableHandle().getColumns())));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        if (!(table instanceof QueryArrowTableHandle)) {
            return super.getTableMetadata(session, table);
        }

        QueryArrowTableHandle queryArrowTableHandle = (QueryArrowTableHandle) table;
        List<ColumnMetadata> meta = ImmutableList.copyOf(Optional.ofNullable(queryArrowTableHandle.getColumns())
                .orElse(new ArrayList<ArrowColumnHandle>())
                .stream()
                .map(column -> {
                    return ColumnMetadata.builder().setName(normalizeIdentifier(session, column.getColumnName())).setType(column.getColumnType()).build();
                })
                .collect(Collectors.toList()));

        return new ConnectorTableMetadata(new SchemaTableName(queryArrowTableHandle.getSchema(), queryArrowTableHandle.getTable()), meta);
    }
}
