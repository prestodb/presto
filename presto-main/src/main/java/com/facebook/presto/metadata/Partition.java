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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;

import static com.facebook.presto.metadata.Util.fromConnectorDomain;
import static com.google.common.base.Preconditions.checkNotNull;

public class Partition
{
    private final String connectorId;
    private final ConnectorPartition connectorPartition;

    public Partition(String connectorId, ConnectorPartition connectorPartition)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.connectorPartition = checkNotNull(connectorPartition, "partition is null");
    }

    public ConnectorPartition getConnectorPartition()
    {
        return connectorPartition;
    }

    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return fromConnectorDomain(connectorId, connectorPartition.getTupleDomain());
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + connectorPartition;
    }

    public static Function<Partition, ConnectorPartition> connectorPartitionGetter()
    {
        return new Function<Partition, ConnectorPartition>()
        {
            @Override
            public ConnectorPartition apply(Partition input)
            {
                return input.getConnectorPartition();
            }
        };
    }

    public static Function<ConnectorPartition, Partition> fromConnectorPartition(final String connectorId)
    {
        return new Function<ConnectorPartition, Partition>()
        {
            @Override
            public Partition apply(ConnectorPartition partition)
            {
                return new Partition(connectorId, partition);
            }
        };
    }
}
