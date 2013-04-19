package com.facebook.presto.spi;

import java.util.Map;

public interface ConnectorFactory
{
    Connector create(String connectorId, Map<String, String> properties);
}
