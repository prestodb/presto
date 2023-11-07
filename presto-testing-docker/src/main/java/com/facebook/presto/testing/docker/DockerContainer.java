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
package com.facebook.presto.testing.docker;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.ContainerNotFoundException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ExecCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedConsumer;

import java.io.Closeable;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public final class DockerContainer
        implements Closeable
{
    private static final Logger LOG = Logger.get(DockerContainer.class);

    private static final boolean DEBUG = false;

    private static final String HOST_IP = "127.0.0.1";
    private final String image;
    private final Map<String, String> environment;
    private DockerClient dockerClient;
    private String containerId;

    private Map<Integer, Integer> hostPorts;

    private Optional<String> cmd = Optional.empty();

    private final boolean exposeTheSamePorts;

    private final Optional<String> hostname;

    public DockerContainer(String image, List<Integer> ports, Map<String, String> environment, CheckedConsumer<HostPortProvider> healthCheck,
            Optional<String> cmd, Optional<String> extraHosts, Optional<String> hostname, boolean exposeTheSamePorts)
    {
        this.image = requireNonNull(image, "image is null");
        this.environment = ImmutableMap.copyOf(requireNonNull(environment, "environment is null"));
        this.cmd = cmd;
        this.exposeTheSamePorts = exposeTheSamePorts;
        this.hostname = hostname;
        try {
            startContainer(ports, healthCheck, extraHosts);
        }
        catch (Exception e) {
            closeAllSuppress(e, this);
            throw new RuntimeException(e);
        }
    }

    public DockerContainer(String image, List<Integer> ports, Map<String, String> environment, CheckedConsumer<HostPortProvider> healthCheck)
    {
        this(image, ports, environment, healthCheck, Optional.empty(), Optional.empty(), Optional.empty(), false);
    }

    private void startContainer(List<Integer> ports, CheckedConsumer<HostPortProvider> healthCheck, Optional<String> extraHosts)
            throws Exception
    {
        dockerClient = DefaultDockerClient.fromEnv().build();
        if (dockerClient.listImages().stream().noneMatch(i -> i.repoTags().contains(image))) {
            checkState(!image.endsWith("-SNAPSHOT"), "Unavailable snapshot image %s, please build before running tests", image);
            LOG.info("Pulling image %s...", image);
            dockerClient.pull(image);
        }
        if (DEBUG) {
            Optional<Container> testingContainer = dockerClient.listContainers().stream()
                    .filter(container -> container.image().equals(image))
                    .collect(toOptional());
            if (testingContainer.isPresent()) {
                containerId = testingContainer.get().id();
                LOG.info("Container for %s already exists with id: %s", image, containerId);
                calculateHostPorts(ports);
            }
            else {
                createContainer(ports, extraHosts, exposeTheSamePorts);
            }
        }
        else {
            createContainer(ports, extraHosts, exposeTheSamePorts);
        }

        checkState(isContainerUp(), "Container was not started properly");

        LOG.info("Auto-assigned host ports are %s", hostPorts);

        waitForContainer(healthCheck);
    }

    private boolean isContainerUp()
    {
        try {
            return dockerClient.inspectContainer(containerId).state().running();
        }
        catch (ContainerNotFoundException e) {
            return false;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createContainer(List<Integer> ports, Optional<String> extraHosts, boolean exposeTheSamePorts)
            throws Exception
    {
        LOG.info("Starting docker container from image %s", image);
        Map<String, List<PortBinding>> portBindings = ports.stream()
                .collect(toImmutableMap(Object::toString, port -> ImmutableList.of(PortBinding.create(HOST_IP, exposeTheSamePorts ? String.valueOf(port) : "0"))));
        Set<String> exposedPorts = ports.stream()
                .map(Object::toString)
                .collect(toImmutableSet());

        HostConfig.Builder hostConfBuilder = HostConfig.builder();
        hostConfBuilder.portBindings(portBindings);
        if (extraHosts.isPresent() && !extraHosts.get().isEmpty()) {
            hostConfBuilder.extraHosts(extraHosts.get());
        }

        ContainerConfig.Builder confBuilder = ContainerConfig.builder()
                .hostConfig(hostConfBuilder.build())
                .exposedPorts(exposedPorts)
                .env(environment.entrySet().stream()
                        .map(entry -> format("%s=%s", entry.getKey(), entry.getValue()))
                        .collect(toImmutableList()))
                .image(image);
        hostname.ifPresent(confBuilder::hostname);
        cmd.ifPresent(confBuilder::cmd);

        containerId = dockerClient.createContainer(confBuilder.build()).id();

        LOG.info("Started docker container with id: %s", containerId);

        dockerClient.startContainer(containerId);

        calculateHostPorts(ports);

        waitForContainerPorts(ports);
    }

    private void createContainer(List<Integer> ports)
            throws Exception
    {
        createContainer(ports, Optional.empty(), false);
    }

    private void waitForContainer(CheckedConsumer<HostPortProvider> healthCheck)
    {
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                .withMaxDuration(Duration.of(10, MINUTES))
                .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                .abortOn(error -> !isContainerUp())
                .onRetry(event -> LOG.info(format("Waiting for container for %s [%s]...", image, event.getLastFailure())))
                .withDelay(Duration.of(10, SECONDS));
        Failsafe.with(retryPolicy).run(() -> healthCheck.accept(this::getHostPort));
    }

    private void waitForContainerPorts(List<Integer> ports)
    {
        List<Integer> hostPorts = ports.stream()
                .map(this::getHostPort)
                .collect(toImmutableList());

        RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                .withMaxDuration(Duration.of(10, MINUTES))
                .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                .abortOn(error -> !isContainerUp())
                .withDelay(Duration.of(5, SECONDS))
                .onRetry(event -> LOG.info("Waiting for ports %s that are exposed on %s on %s ...", ports, HOST_IP, hostPorts));

        Failsafe.with(retryPolicy).run(() -> {
            for (int port : ports) {
                try (Socket socket = new Socket(HOST_IP, getHostPort(port))) {
                    checkState(socket.isConnected());
                }
            }
        });
    }

    private void calculateHostPorts(List<Integer> ports)
            throws Exception
    {
        hostPorts = dockerClient.inspectContainer(containerId).networkSettings()
                .ports()
                .entrySet()
                .stream()
                .filter(entry -> ports.contains(extractPort(entry)))
                .collect(toImmutableMap(
                        entry -> extractPort(entry),
                        entry -> entry.getValue().stream()
                                .peek(portBinding -> {
                                    checkState(portBinding.hostIp().equals(HOST_IP), "Unexpected port binding found: %s", portBinding);
                                })
                                .map(PortBinding::hostPort)
                                .collect(toOptional())
                                .map(Integer::parseInt)
                                .orElseThrow(() -> new IllegalStateException("Could not extract port mapping from: " + entry))));
    }

    public int getHostPort(int port)
    {
        checkArgument(hostPorts.keySet().contains(port), "Port %s is not bound", port);
        return hostPorts.get(port);
    }

    public void executeCmd(String user, String[] cmd)
    {
        try {
            DockerClient.ExecCreateParam userParam = DockerClient.ExecCreateParam.user(user);
            ExecCreation execCreation = dockerClient.execCreate(containerId, cmd, userParam,
                    DockerClient.ExecCreateParam.attachStdout(), DockerClient.ExecCreateParam.attachStderr());
            String execId = execCreation.id();
            LogStream logStream = dockerClient.execStart(execId);
            System.out.println("Command executed successfully in container " + containerId);
        }
        catch (Exception e) {
            LOG.error(e);
        }
    }

    public String getHostname()
    {
        try {
            // Use the DockerClient to inspect the container and get its details
            ContainerInfo containerInfo = dockerClient.inspectContainer(containerId);
            String hostname = containerInfo.config().hostname();
            // Print the hostname
            System.out.println("Hostname of container " + containerId + ": " + hostname);
            return hostname;
        }
        catch (Exception e) {
            LOG.error(e);
            throw new RuntimeException("Failed to get the hostname of container " + containerId, e);
        }
    }

    public String getHostIp()
    {
        try {
            // Use the DockerClient to inspect the container and get its details
            ContainerInfo containerInfo = dockerClient.inspectContainer(containerId);
            String ipAddress = containerInfo.networkSettings().ipAddress();
            // Print the IP address
            System.out.println("IP address of container " + containerId + ": " + ipAddress);
            return ipAddress;
        }
        catch (Exception e) {
            LOG.error(e);
            throw new RuntimeException("Failed to get the IP address of container " + containerId, e);
        }
    }

    private static int extractPort(Entry<String, List<PortBinding>> entry)
    {
        checkArgument(!entry.getKey().contains("/udp"), "UDP port binding is not supported");
        return Integer.parseInt(entry.getKey().replace("/tcp", ""));
    }

    private void removeContainer(String containerId)
    {
        try {
            LOG.info("Killing container %s", containerId);
            dockerClient.killContainer(containerId);
            dockerClient.removeContainer(containerId);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        if (dockerClient == null) {
            return;
        }
        if (!DEBUG && containerId != null) {
            removeContainer(containerId);
        }
        dockerClient.close();
        dockerClient = null;
    }

    public interface HostPortProvider
    {
        int getHostPort(int containerPort);
    }
}
