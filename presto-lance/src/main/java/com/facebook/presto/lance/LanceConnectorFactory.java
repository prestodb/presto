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
package com.facebook.presto.lance;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LanceConnectorFactory
        implements ConnectorFactory
{
    // Properties handled by LanceConfig via @Config annotations.
    // All other properties (including free-form lance.* namespace properties like
    // lance.root, lance.uri, lance.storage.*) are passed through to the namespace
    // implementation via @LanceNamespaceProperties and must NOT be listed here.
    // Note: connector.name is stripped by the Presto framework before reaching create().
    // IMPORTANT: When adding new @Config properties to LanceConfig, update this set.
    private static final Set<String> KNOWN_CONFIG_PROPERTIES = ImmutableSet.of(
            "lance.impl",
            "lance.single-level-ns",
            "lance.parent",
            "lance.read-batch-size",
            "lance.max-rows-per-file",
            "lance.max-rows-per-group",
            "lance.write-batch-size");

    @Override
    public String getName()
    {
        return "lance";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LanceHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        ClassLoader classLoader = LanceConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            // Keep an immutable copy of all config for passing to LanceNamespaceHolder
            Map<String, String> catalogProperties = ImmutableMap.copyOf(config);

            // Filter to only known properties for the Bootstrap configuration.
            // This allows free-form lance.* properties to be passed through without error.
            Map<String, String> knownProperties = new HashMap<>();
            for (Map.Entry<String, String> entry : config.entrySet()) {
                if (KNOWN_CONFIG_PROPERTIES.contains(entry.getKey())) {
                    knownProperties.put(entry.getKey(), entry.getValue());
                }
            }

            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new LanceModule(),
                    binder -> {
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        // Bind the raw namespace properties map for free-form property access
                        binder.bind(new TypeLiteral<Map<String, String>>() {})
                                .annotatedWith(LanceNamespaceProperties.class)
                                .toInstance(catalogProperties);
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(knownProperties)
                    .initialize();

            return injector.getInstance(LanceConnector.class);
        }
    }
}
