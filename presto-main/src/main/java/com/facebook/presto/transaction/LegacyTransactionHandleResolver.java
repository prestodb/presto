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
package com.facebook.presto.transaction;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class LegacyTransactionHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;
    private final ConnectorHandleResolver handleResolver;

    public LegacyTransactionHandleResolver(String connectorId, ConnectorHandleResolver handleResolver)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return handleResolver.canHandle(tableHandle);
    }

    @Override
    public boolean canHandle(ConnectorTableLayoutHandle handle)
    {
        return handleResolver.canHandle(handle);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return handleResolver.canHandle(columnHandle);
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return handleResolver.canHandle(split);
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        return handleResolver.canHandle(indexHandle);
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle tableHandle)
    {
        return handleResolver.canHandle(tableHandle);
    }

    @Override
    public boolean canHandle(ConnectorInsertTableHandle tableHandle)
    {
        return handleResolver.canHandle(tableHandle);
    }

    @Override
    public boolean canHandle(ConnectorTransactionHandle transactionHandle)
    {
        return (transactionHandle instanceof LegacyTransactionHandle) &&
                ((LegacyTransactionHandle) transactionHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return handleResolver.getTableHandleClass();
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return handleResolver.getTableLayoutHandleClass();
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return handleResolver.getColumnHandleClass();
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return handleResolver.getSplitClass();
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        return handleResolver.getIndexHandleClass();
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return handleResolver.getOutputTableHandleClass();
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return handleResolver.getInsertTableHandleClass();
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return LegacyTransactionHandle.class;
    }
}
