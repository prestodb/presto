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
package com.facebook.presto.google.sheets;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class SheetsPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final SheetsClient sheetsClient;

    @Inject
    public SheetsPageSinkProvider(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "sheets client is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            PageSinkContext pageSinkContext)
    {
        SheetsOutputTableHandle handle = (SheetsOutputTableHandle) outputTableHandle;
        return new SheetsPageSink(sheetsClient, handle.getSchemaTableName().getTableName(), handle.getColumns());
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            PageSinkContext pageSinkContext)
    {
        SheetsInsertTableHandle handle = (SheetsInsertTableHandle) insertTableHandle;
        return new SheetsPageSink(sheetsClient, handle.getTableName(), handle.getColumns());
    }
}
