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
package com.facebook.presto.spi;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Result of fetching Partitions in the ConnectorSplitManager interface.
 *
 * Results are comprised of three parts:
 * 1) The actual partitions
 * 2) The TupleDomain that represents the values that the connector was not able to pre-evaluate
 * when generating the partitions and will need to be double-checked by the final execution plan.
 * 3) The domains which are evaluated by the connector.
 */
public class ConnectorPartitionResult
{
    private final List<ConnectorPartition> partitions;
    private final TupleDomain<ConnectorColumnHandle> undeterminedTupleDomain;
    private final PartitionQueryDomain partitionQueryDomains;

    public ConnectorPartitionResult(List<ConnectorPartition> partitions,
                                    TupleDomain<ConnectorColumnHandle> undeterminedTupleDomain)
    {
        this(partitions, undeterminedTupleDomain, new PartitionQueryDomain());
    }

    public ConnectorPartitionResult(List<ConnectorPartition> partitions,
                                    TupleDomain<ConnectorColumnHandle> undeterminedTupleDomain,
                                    PartitionQueryDomain partitionQueryDomains)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.undeterminedTupleDomain = requireNonNull(undeterminedTupleDomain, "undeterminedTupleDomain is null");
        this.partitionQueryDomains = partitionQueryDomains;
    }

    public List<ConnectorPartition> getPartitions()
    {
        return partitions;
    }

    public TupleDomain<ConnectorColumnHandle> getUndeterminedTupleDomain()
    {
        return undeterminedTupleDomain;
    }

    public PartitionQueryDomain getPartitionQueryDomains()
    {
        return partitionQueryDomains;
    }
}
