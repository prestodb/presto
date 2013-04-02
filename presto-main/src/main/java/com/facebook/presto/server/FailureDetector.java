package com.facebook.presto.server;

import io.airlift.discovery.client.ServiceDescriptor;

import java.util.Set;

public interface FailureDetector
{
    Set<ServiceDescriptor> getFailed();
}
