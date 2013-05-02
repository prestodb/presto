package com.facebook.presto.failureDetector;

import io.airlift.discovery.client.ServiceDescriptor;

import java.util.Set;

public interface FailureDetector
{
    Set<ServiceDescriptor> getFailed();
}
