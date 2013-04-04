package com.facebook.presto.server;

import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.Threads;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.stats.DecayCounter;
import io.airlift.stats.ExponentialDecay;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static io.airlift.http.client.Request.Builder.prepareHead;

public class HeartbeatFailureDetector
        implements FailureDetector
{
    private final static Logger log = Logger.get(HeartbeatFailureDetector.class);

    private final ServiceSelector selector;
    private final AsyncHttpClient httpClient;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(Threads.daemonThreadsNamed("failure-detector"));

    // monitoring tasks by service id
    private final ConcurrentMap<UUID, MonitoringTask> tasks = new ConcurrentHashMap<>();

    private final double failureRatioThreshold;
    private final Duration hearbeatInterval;
    private final boolean isEnabled;
    private final Duration warmupInterval;

    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public HeartbeatFailureDetector(@ServiceType("presto") ServiceSelector selector,
            @ForFailureDetector AsyncHttpClient httpClient,
            FailureDetectorConfiguration config,
            QueryManagerConfig queryManagerConfig)
    {
        checkNotNull(selector, "selector is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(config, "config is null");
        checkArgument((long) config.getHearbeatInterval().toMillis() >= 1, "heartbeat interval must be >= 1ms");

        this.selector = selector;
        this.httpClient = httpClient;

        this.failureRatioThreshold = config.getFailureRatioThreshold();
        this.hearbeatInterval = config.getHearbeatInterval();
        this.warmupInterval = config.getWarmupInterval();
        this.isEnabled = config.isEnabled() && queryManagerConfig.isCoordinator();
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
                        log.warn(e, "Error updating services");
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

    @Override
    public Set<ServiceDescriptor> getFailed()
    {
        ImmutableSet.Builder<ServiceDescriptor> builder = ImmutableSet.builder();
        for (MonitoringTask task : tasks.values()) {
            Stats stats = task.getStats();
            if (stats.getRecentFailureRatio() > failureRatioThreshold || stats.getAge().compareTo(warmupInterval) < 0) {
                builder.add(task.getService());
            }
        }
        return builder.build();
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

    public Map<ServiceDescriptor, Stats> getStats()
    {
        ImmutableMap.Builder<ServiceDescriptor, Stats> builder = ImmutableMap.builder();
        for (MonitoringTask task : tasks.values()) {
            builder.put(task.getService(), task.getStats());
        }
        return builder.build();
    }

    private void updateMonitoredServices()
    {
        Set<ServiceDescriptor> active = ImmutableSet.copyOf(selector.selectAllServices());

        Set<UUID> activeIds = IterableTransformer.on(active)
                .transform(idGetter())
                .set();

        synchronized (tasks) { // make sure only one thread is updating the registrations
            Set<UUID> current = tasks.keySet();

            // cancel tasks for all services that are not longer in discovery
            for (UUID serviceId : Sets.difference(current, activeIds)) {
                tasks.remove(serviceId).getFuture().cancel(true);
            }

            // schedule tasks for all new services
            Set<ServiceDescriptor> newServices = IterableTransformer.on(active)
                    .select(compose(not(in(current)), idGetter()))
                    .set();

            for (final ServiceDescriptor service : newServices) {
                final URI uri = getHttpUri(service);

                if (uri != null) {
                    ScheduledFuture<?> future = executor.scheduleAtFixedRate(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try {
                                ping(service, uri);
                            }
                            catch (Throwable e) {
                                // ignore to avoid getting unscheduled
                                log.warn(e, "Error updating services");
                            }
                        }
                    }, (long) hearbeatInterval.toMillis(), (long) hearbeatInterval.toMillis(), TimeUnit.MILLISECONDS);

                    // benign race condition between ping() and registering the MonitoringTask if a ping complets
                    // before this next line executes. This is not an issue because the recordXXX methods check
                    // for the presence of the MonitoringTask in the map
                    tasks.put(service.getId(), new MonitoringTask(future, service, new Stats(uri)));
                }
            }
        }
    }

    private URI getHttpUri(ServiceDescriptor service)
    {
        try {
            String uri = service.getProperties().get("http");
            if (uri != null) {
                return new URI(uri);
            }
        }
        catch (URISyntaxException e) {
            // ignore, not a valid http uri
        }

        return null;
    }

    private Function<ServiceDescriptor, UUID> idGetter()
    {
        return new Function<ServiceDescriptor, UUID>()
        {
            @Override
            public UUID apply(ServiceDescriptor descriptor)
            {
                return descriptor.getId();
            }
        };
    }

    private void ping(final ServiceDescriptor service, final URI uri)
    {
        try {
            recordStart(service);
            httpClient.executeAsync(prepareHead().setUri(uri).build(), new ResponseHandler<Object, Exception>()
            {
                @Override
                public Exception handleException(Request request, Exception exception)
                {
                    // ignore error
                    recordFailure(service, exception);

                    // TODO: this will technically cause an NPE in httpClient, but it's not triggered because
                    // we never call get() on the response future. This behavior needs to be fixed in airlift
                    return null;
                }

                @Override
                public Object handle(Request request, Response response)
                        throws Exception
                {
                    recordSuccess(service);
                    return null;
                }
            });
        }
        catch (Exception e) {
            log.warn(e, "Error scheduling request for %s", uri);
        }
    }

    private void recordStart(ServiceDescriptor service)
    {
        MonitoringTask task = tasks.get(service.getId());
        if (task != null) {
            task.getStats().recordStart();
        }
    }

    private void recordSuccess(ServiceDescriptor service)
    {
        MonitoringTask task = tasks.get(service.getId());
        if (task != null) {
            task.getStats().recordSuccess();
        }
    }

    private void recordFailure(ServiceDescriptor service, Exception exception)
    {
        MonitoringTask task = tasks.get(service.getId());
        if (task != null) {
            task.getStats().recordFailure(exception);
        }
    }

    private static class MonitoringTask
    {
        private final ScheduledFuture<?> future;
        private final ServiceDescriptor service;
        private final Stats stats;

        private MonitoringTask(ScheduledFuture<?> future, ServiceDescriptor service, Stats stats)
        {
            this.future = future;
            this.service = service;
            this.stats = stats;
        }

        public ScheduledFuture<?> getFuture()
        {
            return future;
        }

        public Stats getStats()
        {
            return stats;
        }

        public ServiceDescriptor getService()
        {
            return service;
        }
    }

    public static class Stats
    {
        private final long start = System.nanoTime();
        private final URI uri;

        private final DecayCounter recentRequests = new DecayCounter(ExponentialDecay.oneMinute());
        private final DecayCounter recentFailures = new DecayCounter(ExponentialDecay.oneMinute());
        private final DecayCounter recentSuccesses = new DecayCounter(ExponentialDecay.oneMinute());
        private final AtomicReference<DateTime> lastRequestTime = new AtomicReference<>();
        private final AtomicReference<DateTime> lastResponseTime = new AtomicReference<>();

        @GuardedBy("this")
        private final Map<Class<? extends Throwable>, DecayCounter> failureCountByType = new HashMap<>();

        public Stats(URI uri)
        {
            this.uri = uri;
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
