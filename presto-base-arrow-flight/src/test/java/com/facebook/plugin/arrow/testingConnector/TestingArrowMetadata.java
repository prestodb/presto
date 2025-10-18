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
import com.facebook.plugin.arrow.ArrowFlightConfig;
import com.facebook.plugin.arrow.ArrowMetadata;
import com.facebook.plugin.arrow.BaseArrowFlightClientHandler;
import com.facebook.plugin.arrow.testingConnector.tvf.QueryFunctionProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class TestingArrowMetadata
        extends ArrowMetadata
{
    @Inject
    public TestingArrowMetadata(BaseArrowFlightClientHandler clientHandler, ArrowBlockBuilder arrowBlockBuilder, ArrowFlightConfig config)
    {
        super(clientHandler, arrowBlockBuilder, config);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof QueryFunctionProvider.QueryFunctionHandle) {
            QueryFunctionProvider.QueryFunctionHandle functionHandle = (QueryFunctionProvider.QueryFunctionHandle) handle;
            checkArgument(functionHandle.getTableHandle().getColumns().isPresent(), "Table handle for TVF does not contain any columns");
            return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), new ArrayList<>(functionHandle.getTableHandle().getColumns().get())));
        }
        return Optional.empty();
    }
}
