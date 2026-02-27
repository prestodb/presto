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
package com.facebook.presto.server;

import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.google.inject.Inject;
import javax.inject.Provider;

/**
 * Provides {@link LocalMemoryManager} with coordinator-only validation when this process is a
 * coordinator that does not schedule work on itself (node-scheduler.include-coordinator=false).
 * In that case only heap headroom is validated against the coordinator's JVM heap; the same
 * query.max-memory-per-node and query.max-total-memory-per-node config used for workers need
 * not fit in the coordinator's heap.
 */
public class LocalMemoryManagerProvider
        implements Provider<LocalMemoryManager>
{
    private final NodeMemoryConfig nodeMemoryConfig;
    private final ServerConfig serverConfig;
    private final NodeSchedulerConfig nodeSchedulerConfig;

    @Inject
    public LocalMemoryManagerProvider(
            NodeMemoryConfig nodeMemoryConfig,
            ServerConfig serverConfig,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.nodeMemoryConfig = nodeMemoryConfig;
        this.serverConfig = serverConfig;
        this.nodeSchedulerConfig = nodeSchedulerConfig;
    }

    @Override
    public LocalMemoryManager get()
    {
        long availableMemory = Runtime.getRuntime().maxMemory();
        boolean useCoordinatorOnlyValidation = serverConfig.isCoordinator()
                && !nodeSchedulerConfig.isIncludeCoordinator();
        return new LocalMemoryManager(nodeMemoryConfig, availableMemory, useCoordinatorOnlyValidation);
    }
}
