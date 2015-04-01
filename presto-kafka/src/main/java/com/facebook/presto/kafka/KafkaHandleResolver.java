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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.google.inject.name.Named;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Kafka specific {@link com.facebook.presto.spi.ConnectorHandleResolver} implementation.
 */
public class KafkaHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;

    @Inject
    KafkaHandleResolver(@Named("connectorId") String connectorId,
            KafkaConnectorConfig kafkaConnectorConfig)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        checkNotNull(kafkaConnectorConfig, "kafkaConfig is null");
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandle != null && tableHandle instanceof KafkaTableHandle && connectorId.equals(((KafkaTableHandle) tableHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle != null && columnHandle instanceof KafkaColumnHandle && connectorId.equals(((KafkaColumnHandle) columnHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split != null && split instanceof KafkaSplit && connectorId.equals(((KafkaSplit) split).getConnectorId());
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return KafkaTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return KafkaColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return KafkaSplit.class;
    }

    KafkaTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof KafkaTableHandle, "tableHandle is not an instance of KafkaTableHandle");
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) tableHandle;
        checkArgument(kafkaTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        return kafkaTableHandle;
    }

    KafkaColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof KafkaColumnHandle, "columnHandle is not an instance of KafkaColumnHandle");
        KafkaColumnHandle kafkaColumnHandle = (KafkaColumnHandle) columnHandle;
        checkArgument(kafkaColumnHandle.getConnectorId().equals(connectorId), "columnHandle is not for this connector");
        return kafkaColumnHandle;
    }

    KafkaSplit convertSplit(ConnectorSplit split)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof KafkaSplit, "split is not an instance of KafkaSplit");
        KafkaSplit kafkaSplit = (KafkaSplit) split;
        checkArgument(kafkaSplit.getConnectorId().equals(connectorId), "split is not for this connector");
        return kafkaSplit;
    }
}
