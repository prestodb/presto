package com.facebook.presto.plugin.hbase;


import com.facebook.presto.hbase.HbaseConnectorFactory;
import com.facebook.presto.hbase.HbasePlugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import jersey.repackaged.com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.assertEquals;

public class TestHbasePlugin {
    @Test
    public void getFunctions() {
        HbasePlugin  plugin = new HbasePlugin();
        Set<Class<?>> functions = plugin.getFunctions();
        assertEquals(functions, ImmutableSet.of(), "Expected an empty set of functions");
    }

    @Test
    public void getConnectorFactories() {
        HbasePlugin  plugin = new HbasePlugin();
        Iterable<ConnectorFactory> connectorFactories = plugin.getConnectorFactories();
        assertEquals(ImmutableList.copyOf(connectorFactories), ImmutableList.of(new HbaseConnectorFactory("hbase")), "Expected a list with one HbaseConnectorFactory");
    }
}
