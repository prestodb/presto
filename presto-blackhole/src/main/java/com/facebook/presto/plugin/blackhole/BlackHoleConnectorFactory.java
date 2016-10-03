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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.Map;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class BlackHoleConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "blackhole";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new BlackHoleHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context)
    {
        ListeningScheduledExecutorService executorService = listeningDecorator(newSingleThreadScheduledExecutor(daemonThreadsNamed("blackhole")));
        return new BlackHoleConnector(
                new BlackHoleMetadata(),
                new BlackHoleSplitManager(),
                new BlackHolePageSourceProvider(),
                new BlackHolePageSinkProvider(executorService),
                new BlackHoleNodePartitioningProvider(context.getNodeManager()),
                context.getTypeManager(),
                executorService);
    }
}
