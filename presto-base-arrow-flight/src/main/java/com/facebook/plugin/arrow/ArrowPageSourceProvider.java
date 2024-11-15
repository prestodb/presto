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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

public class ArrowPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger logger = Logger.get(ArrowPageSourceProvider.class);
    private ArrowFlightClientHandler clientHandler;
    @Inject
    public ArrowPageSourceProvider(ArrowFlightClientHandler clientHandler)
    {
        this.clientHandler = clientHandler;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns, SplitContext splitContext)
    {
        ImmutableList.Builder<ArrowColumnHandle> columnHandles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            columnHandles.add((ArrowColumnHandle) handle);
        }
        ArrowSplit arrowSplit = (ArrowSplit) split;
        logger.debug("Processing split with flight ticket");
        return new ArrowPageSource(arrowSplit, columnHandles.build(), clientHandler, session);
    }
}
