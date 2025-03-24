package com.facebook.presto.hbase;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * @author spancer.ray
 *
 */
public class HbaseConnectorFactory implements ConnectorFactory {
  private String name;

  public HbaseConnectorFactory(final String name) {
    checkArgument(!isNullOrEmpty(name), "name is null or empty");
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new HbaseHandleResolver();
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config,
      ConnectorContext context) {
    requireNonNull(connectorId, "connectorId is null");
    requireNonNull(config, "requiredConfig is null");
    requireNonNull(context, "context is null");

    try {
      Bootstrap app = new Bootstrap(new HbaseModule(), binder -> {
        binder.bind(HbaseConnectorId.class).toInstance(new HbaseConnectorId(connectorId));
        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
      });

      Injector injector = app.strictConfig().doNotInitializeLogging()
          .setRequiredConfigurationProperties(config).initialize();

      return injector.getInstance(HbaseConnector.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
