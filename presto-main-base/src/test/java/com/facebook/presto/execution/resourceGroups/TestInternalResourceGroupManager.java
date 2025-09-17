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

package com.facebook.presto.execution.resourceGroups;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.execution.MockManagedQueryExecution;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterResourceChecker;
import com.facebook.presto.execution.scheduler.clusterOverload.CpuMemoryOverloadPolicy;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

public class TestInternalResourceGroupManager
{
    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Presto server is still initializing.*")
    public void testQueryFailsWithInitializingConfigurationManager()
    {
        InternalResourceGroupManager<ImmutableMap<Object, Object>> internalResourceGroupManager = new InternalResourceGroupManager<>((poolId, listener) -> {}, new QueryManagerConfig(), new NodeInfo("test"), new MBeanExporter(new TestingMBeanServer()), () -> null, new ServerConfig(), new InMemoryNodeManager(), new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));
        internalResourceGroupManager.submit(new MockManagedQueryExecution(0), new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), command -> {});
    }

    @Test
    public void testQuerySucceedsWhenConfigurationManagerLoaded()
            throws Exception
    {
        InternalResourceGroupManager<ImmutableMap<Object, Object>> internalResourceGroupManager = new InternalResourceGroupManager<>((poolId, listener) -> {},
                new QueryManagerConfig(), new NodeInfo("test"), new MBeanExporter(new TestingMBeanServer()), () -> null, new ServerConfig(), new InMemoryNodeManager(), new ClusterResourceChecker(new CpuMemoryOverloadPolicy(new ClusterOverloadConfig()), new ClusterOverloadConfig(), new InMemoryNodeManager()));
        internalResourceGroupManager.loadConfigurationManager();
        internalResourceGroupManager.submit(new MockManagedQueryExecution(0), new SelectionContext<>(new ResourceGroupId("global"), ImmutableMap.of()), command -> {});
    }
}
