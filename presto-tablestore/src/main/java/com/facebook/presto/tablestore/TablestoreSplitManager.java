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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TablestoreSplitManager
        implements ConnectorSplitManager
{
    private final TablestoreFacade tablestoreFacade;

    @Inject
    public TablestoreSplitManager(TablestoreFacade tablestoreFacade)
    {
        this.tablestoreFacade = requireNonNull(tablestoreFacade, "tablestoreFacade is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        TablestoreTableLayoutHandle ol = TypeUtil.checkType(layout, TablestoreTableLayoutHandle.class);
        List<? extends ConnectorSplit> x = tablestoreFacade.getSplits(session, ol);
        return new FixedSplitSource(x);
    }
}
