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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NetworkLocationCache;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.util.FinalizerService;
import io.airlift.units.Duration;

import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Decouple node and partition management in presto-main and remove this hack
 */
public class PrestoSparkNodeScheduler
        extends NodeScheduler
{
    public PrestoSparkNodeScheduler()
    {
        super(
                new NetworkLocationCache(new LegacyNetworkTopology()),
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSelectionStats(),
                new NodeSchedulerConfig(),
                new NodeTaskMap(new FinalizerService()),
                new Duration(5, SECONDS));
    }

    @Override
    public NodeSelector createNodeSelector(ConnectorId connectorId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeSelector createNodeSelector(ConnectorId connectorId, int maxTasksPerStage)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, CounterStat> getTopologicalSplitCounters()
    {
        throw new UnsupportedOperationException();
    }
}
