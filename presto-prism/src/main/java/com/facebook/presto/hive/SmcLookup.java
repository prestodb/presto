package com.facebook.presto.hive;

import com.facebook.swift.smc.Service;
import com.facebook.swift.smc.ServiceException;
import com.facebook.swift.smc.ServiceState;
import com.facebook.swift.smc.SmcClient;
import com.facebook.swift.smc.SmcClientProvider;
import com.facebook.swift.smc.Tier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class SmcLookup
{
    private final SmcClientProvider smcClientProvider;

    @Inject
    public SmcLookup(SmcClientProvider smcClientProvider)
    {
        this.smcClientProvider = checkNotNull(smcClientProvider, "smcClientProvider is null");
    }

    public List<HostAndPort> getServices(String tierName)
    {
        try (SmcClient smcClient = smcClientProvider.get()) {
            Tier tier = smcClient.getTierByName(tierName);
            ImmutableList.Builder<HostAndPort> builder = ImmutableList.builder();
            for (Service service : tier.getServices()) {
                if (service.getState().contains(ServiceState.ENABLED)) {
                    builder.add(HostAndPort.fromParts(service.getHostname(), service.getPort()));
                }
            }
            return builder.build();
        }
        catch (ServiceException e) {
            throw Throwables.propagate(e);
        }
    }
}
