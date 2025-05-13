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

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.memory.LocalMemoryManager;
import com.sun.management.OperatingSystemMXBean;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/status")
@RolesAllowed(INTERNAL)
public class StatusResource
{
    private final NodeInfo nodeInfo;
    private final NodeVersion version;
    private final String environment;
    private final boolean coordinator;
    private final long startTime = System.nanoTime();
    private final int logicalCores;
    private final LocalMemoryManager memoryManager;
    private final MemoryMXBean memoryMXBean;

    private OperatingSystemMXBean operatingSystemMXBean;

    @Inject
    public StatusResource(NodeVersion nodeVersion, NodeInfo nodeInfo, ServerConfig serverConfig, LocalMemoryManager memoryManager)
    {
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.coordinator = requireNonNull(serverConfig, "serverConfig is null").isCoordinator();
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.logicalCores = Runtime.getRuntime().availableProcessors();

        if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean) {
            // we want the com.sun.management sub-interface of java.lang.management.OperatingSystemMXBean
            this.operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        }
    }

    @HEAD
    @Produces(APPLICATION_JSON) // to match the GET route
    public Response statusPing()
    {
        return Response.ok().build();
    }

    @GET
    @Produces(APPLICATION_JSON)
    public NodeStatus getStatus()
    {
        return new NodeStatus(
                nodeInfo.getNodeId(),
                version,
                environment,
                coordinator,
                nanosSince(startTime),
                nodeInfo.getExternalAddress(),
                nodeInfo.getInternalAddress(),
                memoryManager.getInfo(),
                logicalCores,
                operatingSystemMXBean == null ? 0 : operatingSystemMXBean.getProcessCpuLoad(),
                operatingSystemMXBean == null ? 0 : operatingSystemMXBean.getSystemCpuLoad(),
                memoryMXBean.getHeapMemoryUsage().getUsed(),
                memoryMXBean.getHeapMemoryUsage().getMax(),
                memoryMXBean.getNonHeapMemoryUsage().getUsed());
    }
}
