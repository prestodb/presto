package com.facebook.presto.event.scribe.client;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.base.Predicate;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.discovery.client.DiscoveryException;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceState;
import io.airlift.discovery.client.ServiceType;

import javax.annotation.PreDestroy;
import javax.inject.Provider;
import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.util.IterableUtils.shuffle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Collections2.filter;
import static java.lang.String.format;

public class ScribeClientProvider
        implements Provider<ScribeClient>, Closeable
{
    private final ThriftClient<ScribeClient> thriftClient;
    private final ServiceSelector selector;
    private final ThriftClientManager thriftClientManager;

    @Inject
    public ScribeClientProvider(
            ThriftClient<ScribeClient> thriftClient,
            @ServiceType("scribe") ServiceSelector serviceSelector,
            ThriftClientManager thriftClientManager
    )
    {
        this.thriftClient = checkNotNull(thriftClient, "thriftClient is null");
        this.selector = checkNotNull(serviceSelector, "serviceSelector is null");
        this.thriftClientManager = thriftClientManager;
    }

    @PreDestroy
    @Override
    public void close()
    {
        // todo: remove this when swift shutdown is fixed
        thriftClientManager.close();
    }

    @Override
    public ScribeClient get()
    {
        Collection<ServiceDescriptor> runningServices = filter(selector.selectAllServices(), runningPredicate());

        if (runningServices.isEmpty()) {
            throw new DiscoveryException(format("No scribe servers available for pool '%s'", selector.getPool()));
        }

        Throwable lastException = null;
        for (ServiceDescriptor service : shuffle(runningServices)) {
            String thrift = service.getProperties().get("thrift");
            if (thrift != null) {
                try {
                    HostAndPort thriftEndpoint = HostAndPort.fromString(thrift);
                    checkArgument(thriftEndpoint.hasPort());
                    return thriftClient.open(new FramedClientConnector(thriftEndpoint)).get();
                }
                catch (IllegalArgumentException ignored) {
                    // Ignore entries with parse issues
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted", e);
                }
                catch (ExecutionException e) {
                    lastException = e.getCause();
                }
            }
        }
        throw new DiscoveryException("Unable to connect to any scribe servers", lastException);
    }

    private static Predicate<? super ServiceDescriptor> runningPredicate()
    {
        return new Predicate<ServiceDescriptor>()
        {
            @Override
            public boolean apply(ServiceDescriptor input)
            {
                return input.getState() != ServiceState.STOPPED;
            }
        };
    }
}
