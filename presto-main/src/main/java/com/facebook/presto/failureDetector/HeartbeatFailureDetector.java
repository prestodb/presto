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
package com.facebook.presto.failureDetector;

import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.airlift.discovery.client.ServiceSelector;
import com.facebook.airlift.discovery.client.ServiceType;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.stats.DecayCounter;
import com.facebook.airlift.stats.ExponentialDecay;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.util.Failures;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareHead;
import static com.facebook.presto.failureDetector.FailureDetector.State.ALIVE;
import static com.facebook.presto.failureDetector.FailureDetector.State.GONE;
import static com.facebook.presto.failureDetector.FailureDetector.State.UNKNOWN;
import static com.facebook.presto.failureDetector.FailureDetector.State.UNRESPONSIVE;
import static com.facebook.presto.spi.HostAddress.fromUri;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class HeartbeatFailureDetector
        implements FailureDetector
{
    private static final Logger log = Logger.get(HeartbeatFailureDetector.class);

    private final ServiceSelector selector;
    private final HttpClient httpClient;
    private final NodeInfo nodeInfo;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("failure-detector"));
    private final ThreadPoolExecutorMBean executorMBean = new ThreadPoolExecutorMBean(executor);

    // monitoring tasks by service id
    private final ConcurrentMap<UUID, MonitoringTask> tasks = new ConcurrentHashMap<>();

    private final double failureRatioThreshold;
    private final Duration heartbeat;
    private final boolean isEnabled;
    private final Duration warmupInterval;
    private final Duration gcGraceInterval;
    private final int exponentialDecaySeconds;
    private final boolean httpsRequired;

    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public HeartbeatFailureDetector(
            @ServiceType("presto") ServiceSelector selector,
            @ForFailureDetector HttpClient httpClient,
            FailureDetectorConfig failureDetectorConfig,
            NodeInfo nodeInfo,
            InternalCommunicationConfig internalCommunicationConfig)
    {
        requireNonNull(selector, "selector is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(failureDetectorConfig, "config is null");
        checkArgument(failureDetectorConfig.getHeartbeatInterval().toMillis() >= 1, "heartbeat interval must be >= 1ms");

        this.selector = selector;
        this.httpClient = httpClient;
        this.nodeInfo = nodeInfo;

        this.failureRatioThreshold = failureDetectorConfig.getFailureRatioThreshold();
        this.heartbeat = failureDetectorConfig.getHeartbeatInterval();
        this.warmupInterval = failureDetectorConfig.getWarmupInterval();
        this.gcGraceInterval = failureDetectorConfig.getExpirationGraceInterval();
        this.exponentialDecaySeconds = failureDetectorConfig.getExponentialDecaySeconds();

        this.isEnabled = failureDetectorConfig.isEnabled();

        this.httpsRequired = internalCommunicationConfig.isHttpsRequired();
    }

    @PostConstruct
    public void start()
    {
        if (isEnabled && started.compareAndSet(false, true)) {
            executor.scheduleWithFixedDelay(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        updateMonitoredServices();
                    }
                    catch (Throwable e) {
                        // ignore to avoid getting unscheduled
                        log.error(e, "Error updating services");
                    }
                }
            }, 0, 5, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Override
    public Set<ServiceDescriptor> getFailed()
    {
        return tasks.values().stream()
                .filter(MonitoringTask::isFailed)
                .map(MonitoringTask::getService)
                .collect(toImmutableSet());
    }

    @Override
    public State getState(HostAddress hostAddress)
    {
        for (MonitoringTask task : tasks.values()) {
            if (hostAddress.equals(fromUri(task.uri))) {
                if (!task.isFailed()) {
                    return ALIVE;
                }

                Exception lastFailureException = task.getStats().getLastFailureException();
                if (lastFailureException instanceof ConnectException) {
                    return GONE;
                }
                if (lastFailureException instanceof SocketTimeoutException) {
                    // TODO: distinguish between process unresponsiveness (e.g GC pause) and host reboot
                    return UNRESPONSIVE;
                }

                return UNKNOWN;
            }
        }

        return UNKNOWN;
    }

    @Managed(description = "Number of failed services")
    public int getFailedCount()
    {
        return getFailed().size();
    }

    @Managed(description = "Total number of known services")
    public int getTotalCount()
    {
        return tasks.size();
    }

    @Managed
    public int getActiveCount()
    {
        return tasks.size() - getFailed().size();
    }

    public Map<ServiceDescriptor, Stats> getStats()
    {
        ImmutableMap.Builder<ServiceDescriptor, Stats> builder = ImmutableMap.builder();
        for (MonitoringTask task : tasks.values()) {
            builder.put(task.getService(), task.getStats());
        }
        return builder.build();
    }

    @VisibleForTesting
    void updateMonitoredServices()
    {
        Set<ServiceDescriptor> online = selector.selectAllServices().stream()
                .filter(descriptor -> !nodeInfo.getNodeId().equals(descriptor.getNodeId()))
                .collect(toImmutableSet());

        Set<UUID> onlineIds = online.stream()
                .map(ServiceDescriptor::getId)
                .collect(toImmutableSet());

        // make sure only one thread is updating the registrations
        synchronized (tasks) {
            // 1. remove expired tasks
            List<UUID> expiredIds = tasks.values().stream()
                    .filter(MonitoringTask::isExpired)
                    .map(MonitoringTask::getService)
                    .map(ServiceDescriptor::getId)
                    .collect(toImmutableList());

            tasks.keySet().removeAll(expiredIds);

            // 2. disable offline services
            tasks.values().stream()
                    .filter(task -> !onlineIds.contains(task.getService().getId()))
                    .forEach(MonitoringTask::disable);

            // 3. create tasks for new services
            Set<ServiceDescriptor> newServices = online.stream()
                    .filter(service -> !tasks.keySet().contains(service.getId()))
                    .collect(toImmutableSet());

            for (ServiceDescriptor service : newServices) {
                URI uri = getHttpUri(service);

                if (uri != null) {
                    URI pingURI = uriBuilderFrom(uri).appendPath("/v1/status").build();
                    tasks.put(service.getId(), new MonitoringTask(service, pingURI));
                }
            }

            // 4. enable all online tasks (existing plus newly created)
            tasks.values().stream()
                    .filter(task -> onlineIds.contains(task.getService().getId()))
                    .forEach(MonitoringTask::enable);
        }
    }

    private URI getHttpUri(ServiceDescriptor descriptor)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            try {
                return new URI(url);
            }
            catch (URISyntaxException ignored) {
            }
        }
        return null;
    }

    @ThreadSafe
    private class MonitoringTask
    {
        private final ServiceDescriptor service;
        private final URI uri;
        private final Stats stats;

        @GuardedBy("this")
        private ScheduledFuture<?> future;

        @GuardedBy("this")
        private Long disabledTimestamp;

        @GuardedBy("this")
        private Long successTransitionTimestamp;

        private MonitoringTask(ServiceDescriptor service, URI uri)
        {
            this.uri = uri;
            this.service = service;
            this.stats = new Stats(uri, exponentialDecaySeconds);
        }

        public Stats getStats()
        {
            return stats;
        }

        public ServiceDescriptor getService()
        {
            return service;
        }

        public synchronized void enable()
        {
            if (future == null) {
                future = executor.scheduleAtFixedRate(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try {
                            ping();
                            updateState();
                        }
                        catch (Throwable e) {
                            // ignore to avoid getting unscheduled
                            log.error(e, "Error pinging service %s (%s)", service.getId(), uri);
                        }
                    }
                }, heartbeat.toMillis(), heartbeat.toMillis(), TimeUnit.MILLISECONDS);
                disabledTimestamp = null;
            }
        }

        public synchronized void disable()
        {
            if (future != null) {
                future.cancel(true);
                future = null;
                disabledTimestamp = System.nanoTime();
            }
        }

        public synchronized boolean isExpired()
        {
            return future == null && disabledTimestamp != null && Duration.nanosSince(disabledTimestamp).compareTo(gcGraceInterval) > 0;
        }

        public synchronized boolean isFailed()
        {
            return future == null || // are we disabled?
                    successTransitionTimestamp == null || // are we in success state?
                    Duration.nanosSince(successTransitionTimestamp).compareTo(warmupInterval) < 0; // are we within the warmup period?
        }

        private void ping()
        {
            try {
                stats.recordStart();
                httpClient.executeAsync(prepareHead().setUri(uri).build(), new ResponseHandler<Object, Exception>()
                {
                    @Override
                    public Exception handleException(Request request, Exception exception)
                    {
                        // ignore error
                        stats.recordFailure(exception);

                        // TODO: this will technically cause an NPE in httpClient, but it's not triggered because
                        // we never call get() on the response future. This behavior needs to be fixed in airlift
                        return null;
                    }

                    @Override
                    public Object handle(Request request, Response response)
                    {
                        stats.recordSuccess();
                        return null;
                    }
                });
            }
            catch (RuntimeException e) {
                log.warn(e, "Error scheduling request for %s", uri);
            }
        }

        private synchronized void updateState()
        {
            // is this an over/under transition?
            if (stats.getRecentFailureRatio() > failureRatioThreshold) {
                successTransitionTimestamp = null;
            }
            else if (successTransitionTimestamp == null) {
                successTransitionTimestamp = System.nanoTime();
            }
        }
    }

    public static class Stats
    {
        private final long start = System.nanoTime();
        private final URI uri;

        private final DecayCounter recentRequests;
        private final DecayCounter recentFailures;
        private final DecayCounter recentSuccesses;
        private final AtomicReference<DateTime> lastRequestTime = new AtomicReference<>();
        private final AtomicReference<DateTime> lastResponseTime = new AtomicReference<>();
        private final AtomicReference<Exception> lastFailureException = new AtomicReference<>();

        @GuardedBy("this")
        private final Map<Class<? extends Throwable>, DecayCounter> failureCountByType = new HashMap<>();

        public Stats(URI uri, int exponentialDecaySeconds)
        {
            this.uri = uri;
            this.recentRequests = new DecayCounter(ExponentialDecay.seconds(exponentialDecaySeconds));
            this.recentFailures = new DecayCounter(ExponentialDecay.seconds(exponentialDecaySeconds));
            this.recentSuccesses = new DecayCounter(ExponentialDecay.seconds(exponentialDecaySeconds));
        }

        public void recordStart()
        {
            recentRequests.add(1);
            lastRequestTime.set(new DateTime());
        }

        public void recordSuccess()
        {
            recentSuccesses.add(1);
            lastResponseTime.set(new DateTime());
        }

        public void recordFailure(Exception exception)
        {
            recentFailures.add(1);
            lastResponseTime.set(new DateTime());
            lastFailureException.set(exception);

            Throwable cause = exception;
            while (cause.getClass() == RuntimeException.class && cause.getCause() != null) {
                cause = cause.getCause();
            }

            synchronized (this) {
                DecayCounter counter = failureCountByType.get(cause.getClass());
                if (counter == null) {
                    counter = new DecayCounter(ExponentialDecay.oneMinute());
                    failureCountByType.put(cause.getClass(), counter);
                }
                counter.add(1);
            }
        }

        @JsonProperty
        public Duration getAge()
        {
            return Duration.nanosSince(start);
        }

        @JsonProperty
        public URI getUri()
        {
            return uri;
        }

        @JsonProperty
        public double getRecentFailures()
        {
            return recentFailures.getCount();
        }

        @JsonProperty
        public double getRecentSuccesses()
        {
            return recentSuccesses.getCount();
        }

        @JsonProperty
        public double getRecentRequests()
        {
            return recentRequests.getCount();
        }

        @JsonProperty
        public double getRecentFailureRatio()
        {
            return recentFailures.getCount() / recentRequests.getCount();
        }

        @JsonProperty
        public DateTime getLastRequestTime()
        {
            return lastRequestTime.get();
        }

        @JsonProperty
        public DateTime getLastResponseTime()
        {
            return lastResponseTime.get();
        }

        @JsonIgnore
        public Exception getLastFailureException()
        {
            return lastFailureException.get();
        }

        @Nullable
        @JsonProperty
        public FailureInfo getLastFailureInfo()
        {
            Exception lastFailureException = getLastFailureException();
            if (lastFailureException == null) {
                return null;
            }
            return Failures.toFailure(lastFailureException).toFailureInfo();
        }

        @JsonProperty
        public synchronized Map<String, Double> getRecentFailuresByType()
        {
            ImmutableMap.Builder<String, Double> builder = ImmutableMap.builder();
            for (Map.Entry<Class<? extends Throwable>, DecayCounter> entry : failureCountByType.entrySet()) {
                builder.put(entry.getKey().getName(), entry.getValue().getCount());
            }
            return builder.build();
        }
    }
}
