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

package com.facebook.presto.plugin.nullconnector;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.google.common.base.Throwables.propagate;

public class NullConnectorFactory
        implements ConnectorFactory
{
    private Map<String, String> optionalConfig;

    public NullConnectorFactory(Map<String, String> optionalConfig)
    {
        this.optionalConfig = optionalConfig;
    }

    @Override
    public String getName()
    {
        return "null";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig)
    {
        try {
            Bootstrap app = new Bootstrap(new NullModule());

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(NullConnector.class);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }
}
