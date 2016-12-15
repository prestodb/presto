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
package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;
import static java.util.Objects.requireNonNull;

/**
 * presto-hdfs
 *
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSConnectorFactory
implements ConnectorFactory
{
    public HDFSConnectorFactory()
    {
    }

    @Override
    public String getName()
    {
        return "hdfs";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new HDFSHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new HDFSModule(connectorId, config, context.getTypeManager())
            );

            // TODO restore strictConfig, which leads to config kvs not used error
            Injector injector = app
//                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(HDFSConnector.class);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
