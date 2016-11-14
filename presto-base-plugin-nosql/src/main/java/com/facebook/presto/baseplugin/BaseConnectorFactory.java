package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseConnectorFactory implements ConnectorFactory{
    private final String name;

    private final Class<? extends BaseConfig> baseConfigClass;
    private final Class<? extends BaseConnector> baseConnectorClass;
    private final Class<? extends BaseMetadata> baseMetadataClass;
    private final Class<? extends BaseSplitManager> baseSplitManagerClass;
    private final Class<? extends BaseRecordSetProvider> baseRecordSetProviderClass;
    private final Class<? extends BaseHandleResolver> baseHandleResolverClass;
    private final Class<? extends BaseProvider> baseProviderClass;

    public BaseConnectorFactory(
            String name,
            Class<? extends BaseConfig> baseConfigClass,
            Class<? extends BaseConnector> baseConnectorClass,
            Class<? extends BaseMetadata> baseMetadataClass,
            Class<? extends BaseSplitManager> baseSplitManagerClass,
            Class<? extends BaseRecordSetProvider> baseRecordSetProviderClass,
            Class<? extends BaseHandleResolver> baseHandleResolverClass,
            Class<? extends BaseProvider> baseProviderClass
    ) {
        this.name = requireNonNull(name, "connectorFactory name is null");

        this.baseConfigClass = requireNonNull(baseConfigClass, "baseConfig is null");
        this.baseConnectorClass = requireNonNull(baseConnectorClass, "baseConnector is null");
        this.baseMetadataClass = requireNonNull(baseMetadataClass, "baseMetadata is null");
        this.baseSplitManagerClass = requireNonNull(baseSplitManagerClass, "baseSplitManager is null");
        this.baseRecordSetProviderClass = requireNonNull(baseRecordSetProviderClass, "baseRecordSetProvider is null");
        this.baseHandleResolverClass = requireNonNull(baseHandleResolverClass, "baseHandleResolver is null");
        this.baseProviderClass = requireNonNull(baseProviderClass, "baseProvider is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new BaseHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext connectorContext)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new BaseModule(
                            connectorContext.getNodeManager(),
                            new BaseConnectorInfo(connectorId),
                            baseConfigClass,
                            baseConnectorClass,
                            baseMetadataClass,
                            baseSplitManagerClass,
                            baseRecordSetProviderClass,
                            baseHandleResolverClass,
                            baseProviderClass));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(BaseConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
