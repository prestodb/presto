/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.plugin.arrow;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableList;
import com.google.inject.ConfigurationException;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.List;
import java.util.Map;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.inject.util.Modules.override;
import static java.util.Objects.requireNonNull;

public class ArrowConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module module;
    private final ImmutableList<Module> extraModules;
    private final ClassLoader classLoader;

    public ArrowConnectorFactory(String name, Module module, List<Module> extraModules, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
        this.extraModules = ImmutableList.copyOf(requireNonNull(extraModules, "extraModules is null"));
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ArrowHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                    .add(binder -> {
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    })
                    .add(override(new ArrowModule(catalogName)).with(module))
                    .addAll(extraModules)
                    .build());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(ArrowConnector.class);
        }
        catch (ConfigurationException ex) {
            throw new ArrowException(ARROW_INTERNAL_ERROR, "The connector instance could not be created.", ex);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
