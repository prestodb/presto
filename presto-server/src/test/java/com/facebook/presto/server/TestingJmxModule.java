package com.facebook.presto.server;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.management.MBeanServer;

public class TestingJmxModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());
    }
}
