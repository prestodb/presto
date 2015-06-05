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
package com.facebook.presto.kinesis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.inject.Inject;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.google.inject.name.Named;

public class KinesisHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;

    @Inject
    KinesisHandleResolver(@Named("connectorId") String connectorId,
            KinesisConnectorConfig kinesisConnectorConfig)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        checkNotNull(kinesisConnectorConfig, "kinesisConfig is null");
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandle != null && tableHandle instanceof KinesisTableHandle && connectorId.equals(((KinesisTableHandle) tableHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle != null && columnHandle instanceof KinesisColumnHandle && connectorId.equals(((KinesisColumnHandle) columnHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split != null && split instanceof KinesisSplit && connectorId.equals(((KinesisSplit) split).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        return false;
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle tableHandle)
    {
        return false;
    }

    @Override
    public boolean canHandle(ConnectorInsertTableHandle tableHandle)
    {
        return false;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return KinesisTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return KinesisColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return KinesisSplit.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    KinesisTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof KinesisTableHandle, "tableHandle is not an instance of KinesisTableHandle");
        KinesisTableHandle kinesisTableHandle = (KinesisTableHandle) tableHandle;
        checkArgument(kinesisTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        return kinesisTableHandle;
    }

    KinesisColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof KinesisColumnHandle, "columnHandle is not an instance of KinesisColumnHandle");
        KinesisColumnHandle kinesisColumnHandle = (KinesisColumnHandle) columnHandle;
        checkArgument(kinesisColumnHandle.getConnectorId().equals(connectorId), "columnHandle is not for this connector");
        return kinesisColumnHandle;
    }

    KinesisSplit convertSplit(ConnectorSplit split)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof KinesisSplit, "split is not an instance of KinesisSplit");
        KinesisSplit kinesisSplit = (KinesisSplit) split;
        checkArgument(kinesisSplit.getConnectorId().equals(connectorId), "split is not for this connector");
        return kinesisSplit;
    }
}
