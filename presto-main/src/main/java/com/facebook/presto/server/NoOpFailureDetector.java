package com.facebook.presto.server;

import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.ServiceDescriptor;

import java.util.Set;

public class NoOpFailureDetector
    implements FailureDetector
{
    @Override
    public Set<ServiceDescriptor> getFailed()
    {
        return ImmutableSet.of();
    }
}
