package com.facebook.presto.spi;

import java.util.Map;

public interface IConnectorManager {
	 public ConnectorId createConnection(String catalogName, String connectorName, Map<String, String> properties);
}
