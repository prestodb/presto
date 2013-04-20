package com.facebook.presto.spi;

import java.util.Map;

public interface ConnectorFactory
{
    String getName();

    Connector create(String connectorId, Map<String, String> config);
}
