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
package com.facebook.presto.lark.sheets;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.lark.sheets.api.InMemorySchemaStore;
import com.facebook.presto.lark.sheets.api.LarkSheetsApi;
import com.facebook.presto.lark.sheets.api.LarkSheetsApiFactory;
import com.facebook.presto.lark.sheets.api.LarkSheetsSchemaStore;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.TypeLiteral;

import java.util.Map;
import java.util.function.Supplier;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.Scopes.SINGLETON;

public class LarkSheetsConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "lark-sheets";

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LarkSheetsHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new Bootstrap(
                new JsonModule(),
                binder -> {
                    configBinder(binder).bindConfig(LarkSheetsConfig.class);

                    binder.bind(LarkSheetsConnector.class).in(SINGLETON);
                    binder.bind(LarkSheetsTransactionManager.class).in(SINGLETON);
                    binder.bind(LarkSheetsSchemaProperties.class).in(SINGLETON);
                    binder.bind(new TypeLiteral<Supplier<LarkSheetsApi>>() {}).to(LarkSheetsApiFactory.class).in(SINGLETON);
                    binder.bind(LarkSheetsSchemaStore.class).to(InMemorySchemaStore.class).in(SINGLETON);
                    binder.bind(LarkSheetsSplitManager.class).in(SINGLETON);
                    binder.bind(LarkSheetsRecordSetProvider.class).in(SINGLETON);
                })
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize()
                .getInstance(LarkSheetsConnector.class);
    }
}
