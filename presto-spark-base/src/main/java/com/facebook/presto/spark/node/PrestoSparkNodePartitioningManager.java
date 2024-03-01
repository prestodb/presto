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
package com.facebook.presto.spark.node;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PartitioningScheme;

import javax.inject.Inject;

import java.util.List;

/**
 * TODO: Decouple node and partition management in presto-main and remove this hack
 */
public class PrestoSparkNodePartitioningManager
        extends NodePartitioningManager
{
    @Inject
    public PrestoSparkNodePartitioningManager(PartitioningProviderManager partitioningProviderManager)
    {
        super(new PrestoSparkNodeScheduler(), partitioningProviderManager, new NodeSelectionStats());
    }

    @Override
    public PartitionFunction getPartitionFunction(Session session, PartitioningScheme partitioningScheme, List<Type> partitionChannelTypes)
    {
        return super.getPartitionFunction(session, partitioningScheme, partitionChannelTypes);
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(Session session, PartitioningHandle partitioningHandle)
    {
        return super.listPartitionHandles(session, partitioningHandle);
    }

    @Override
    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle)
    {
        throw new UnsupportedOperationException("grouped execution is not supported in presto on spark");
    }

    @Override
    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, boolean preferDynamic)
    {
        throw new UnsupportedOperationException("grouped execution is not supported in presto on spark");
    }
}
