package com.facebook.presto.plugin.mysql;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcConnectorFactory;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.JdbcModule;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class MySqlConnectorFactory
    extends JdbcConnectorFactory
{
    public MySqlConnectorFactory(String name, Module module, ClassLoader classLoader)
    {
        super(name, module, classLoader);
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                    },
                    new JdbcModule(catalogName),
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(JdbcConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
