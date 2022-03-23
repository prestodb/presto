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
package com.facebook.presto.testing.containers;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HostAndPort.fromParts;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

public abstract class BaseTestContainer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(BaseTestContainer.class);

    private final String hostName;
    private final Set<Integer> ports;
    private final Map<String, String> filesToMount;
    private final Map<String, String> envVars;
    private final Optional<Network> network;
    private final int startupRetryLimit;

    private final GenericContainer<?> container;

    protected BaseTestContainer(
            String image,
            String hostName,
            Set<Integer> ports,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int startupRetryLimit)
    {
        checkArgument(startupRetryLimit > 0, "startupRetryLimit needs to be greater or equal to 0");
        this.container = new GenericContainer<>(requireNonNull(image, "image is null"));
        this.ports = requireNonNull(ports, "ports is null");
        this.hostName = requireNonNull(hostName, "hostName is null");
        this.filesToMount = requireNonNull(filesToMount, "filesToMount is null");
        this.envVars = requireNonNull(envVars, "envVars is null");
        this.network = requireNonNull(network, "network is null");
        this.startupRetryLimit = startupRetryLimit;
        setupContainer();
    }

    protected void setupContainer()
    {
        for (int port : this.ports) {
            container.addExposedPort(port);
        }
        filesToMount.forEach(this::copyFileToContainer);
        container.withEnv(envVars);
        container.withCreateContainerCmdModifier(c -> c.withHostName(hostName))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(Duration.ofMinutes(5));
        network.ifPresent(container::withNetwork);
    }

    protected void withRunCommand(List<String> runCommand)
    {
        container.withCommand(runCommand.toArray(new String[runCommand.size()]));
    }

    protected void copyFileToContainer(String resourcePath, String dockerPath)
    {
        container.withCopyFileToContainer(
                forHostPath(
                        forClasspathResource(resourcePath)
                                // Container fails to mount jar:file:/<host_path>!<resource_path> resources
                                // This assures that JAR resources are being copied out to tmp locations
                                // and mounted from there.
                                .getResolvedPath()),
                dockerPath);
    }

    protected void startContainer()
    {
        Failsafe.with(new RetryPolicy<>()
                .withMaxRetries(startupRetryLimit)
                .onRetry(event -> log.warn(
                        "%s initialization failed (attempt %s), will retry. Exception: %s",
                        this.getClass().getSimpleName(),
                        event.getAttemptCount(),
                        event.getLastFailure().getMessage())))
                .get(() -> TestContainers.startOrReuse(this.container));
    }

    protected HostAndPort getMappedHostAndPortForExposedPort(int exposedPort)
    {
        return fromParts(container.getHost(), container.getMappedPort(exposedPort));
    }

    public void start()
    {
        setupContainer();
        startContainer();
    }

    public void stop()
    {
        container.stop();
    }

    @Override
    public void close()
    {
        stop();
    }

    protected abstract static class Builder<S extends BaseTestContainer.Builder, B extends BaseTestContainer>
    {
        protected String image;
        protected String hostName;
        protected Set<Integer> exposePorts = ImmutableSet.of();
        protected Map<String, String> filesToMount = ImmutableMap.of();
        protected Map<String, String> envVars = ImmutableMap.of();
        protected Optional<Network> network = Optional.empty();
        protected int startupRetryLimit = 1;

        protected S self;

        @SuppressWarnings("unchecked")
        public Builder()
        {
            this.self = (S) this;
        }

        public S withImage(String image)
        {
            this.image = image;
            return self;
        }

        public S withHostName(String hostName)
        {
            this.hostName = hostName;
            return self;
        }

        public S withExposePorts(Set<Integer> exposePorts)
        {
            this.exposePorts = exposePorts;
            return self;
        }

        public S withFilesToMount(Map<String, String> filesToMount)
        {
            this.filesToMount = filesToMount;
            return self;
        }

        public S withEnvVars(Map<String, String> envVars)
        {
            this.envVars = envVars;
            return self;
        }

        public S withNetwork(Network network)
        {
            this.network = Optional.of(network);
            return self;
        }

        public S withStartupRetryLimit(int startupRetryLimit)
        {
            this.startupRetryLimit = startupRetryLimit;
            return self;
        }

        public abstract B build();
    }
}
