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
package com.facebook.presto.delta;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.gcs.HiveGcsModule;
import com.facebook.presto.hive.metastore.HiveMetastoreModule;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.inject.Injector;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class DeltaConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "delta";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new DeltaConnectionHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new DeltaModule(catalogName, context.getTypeManager()),
                    new HiveS3Module(catalogName),
                    new HiveGcsModule(),
                    new HiveAuthenticationModule(),
                    new HiveMetastoreModule(catalogName, Optional.empty()),
                    binder -> {
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(DeltaConnector.class);
        }
        catch (Exception exception) {
            throwIfUnchecked(exception);
            throw new RuntimeException(exception);
        }
    }
}
