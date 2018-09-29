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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final KafkaMetadataClient restMetadataClient;

    @Inject
    public KafkaSplitManager(KafkaConnectorId connectorId,
            KafkaMetadataClient restMetadataClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.restMetadataClient = requireNonNull(restMetadataClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle,
            ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        KafkaTableLayoutHandle layoutHandle = (KafkaTableLayoutHandle) layout;
        KafkaTableHandle tableHandle = layoutHandle.getTable();
        KafkaTable table = restMetadataClient.getTable(tableHandle.getSchemaName(),
                tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists",
                tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new KafkaSplit(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                table.getEntityName(),
                layoutHandle.getTupleDomain()));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
