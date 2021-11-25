package com.facebook.presto.plugin.clickhouse;

import static com.google.common.collect.Iterables.getOnlyElement;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestClickHousePlugin {
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new ClickHousePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:clickhouse://test"), new TestingConnectorContext());
    }
}
