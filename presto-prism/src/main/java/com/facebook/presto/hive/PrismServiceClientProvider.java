package com.facebook.presto.hive;

import com.facebook.nifty.client.UnframedClientConnector;
import com.facebook.prism.namespaceservice.PrismServiceClient;
import com.facebook.swift.service.ThriftClient;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@ThreadSafe
public class PrismServiceClientProvider
{
    private final ThriftClient<PrismServiceClient> thriftClient;
    private final SmcLookup smcLookup;
    private final String prismSmcTier;

    @Inject
    public PrismServiceClientProvider(
            ThriftClient<PrismServiceClient> thriftClient,
            SmcLookup smcLookup,
            PrismConfig config)
    {
        this.thriftClient = checkNotNull(thriftClient, "thriftClient is null");
        this.smcLookup = checkNotNull(smcLookup, "smcLookup is null");
        prismSmcTier = checkNotNull(config, "config is null").getPrismSmcTier();
    }

    public PrismServiceClient get()
    {
        List<HostAndPort> services = smcLookup.getServices(prismSmcTier);
        if (services.isEmpty()) {
            throw new RuntimeException(format("No prism servers available for tier '%s'", prismSmcTier));
        }

        Throwable lastException = null;
        for (HostAndPort service : shuffle(services)) {
            try {
                return thriftClient.open(new UnframedClientConnector(service)).get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
            catch (ExecutionException e) {
                lastException = e.getCause();
            }
        }
        throw new RuntimeException("Unable to connect to any prism servers", lastException);
    }

    private static <T> List<T> shuffle(Iterable<T> iterable)
    {
        List<T> list = Lists.newArrayList(iterable);
        Collections.shuffle(list);
        return list;
    }
}
