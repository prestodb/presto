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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.google.common.base.Throwables.propagate;
import static java.util.Objects.requireNonNull;

public class BlackHoleConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;

    public BlackHoleConnectorFactory(TypeManager typeManager, Map<String, String> optionalConfig)
    {
        this.typeManager = requireNonNull(typeManager);
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig));
    }

    @Override
    public String getName()
    {
        return "blackhole";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig)
    {
        try {
            Bootstrap app = new Bootstrap(new BlackHoleModule(typeManager));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(BlackHoleConnector.class);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }
}
