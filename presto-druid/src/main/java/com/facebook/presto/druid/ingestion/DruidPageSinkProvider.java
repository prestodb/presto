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
package com.facebook.presto.druid.ingestion;

import com.facebook.presto.druid.DruidClient;
import com.facebook.presto.druid.DruidConfig;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class DruidPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final DruidConfig druidConfig;
    private final DruidClient druidClient;
    private final DruidPageWriter druidPageWriter;

    @Inject
    public DruidPageSinkProvider(
            DruidConfig druidConfig,
            DruidClient druidClient,
            DruidPageWriter druidPageWriter)
    {
        this.druidConfig = requireNonNull(druidConfig, "druid config is null");
        this.druidClient = requireNonNull(druidClient, "druid client is null");
        this.druidPageWriter = requireNonNull(druidPageWriter, "page writer is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        DruidIngestionTableHandle tableHandle = (DruidIngestionTableHandle) outputTableHandle;
        return new DruidPageSink(druidConfig, druidClient, tableHandle, druidPageWriter);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        DruidIngestionTableHandle tableHandle = (DruidIngestionTableHandle) insertTableHandle;
        return new DruidPageSink(druidConfig, druidClient, tableHandle, druidPageWriter);
    }
}
