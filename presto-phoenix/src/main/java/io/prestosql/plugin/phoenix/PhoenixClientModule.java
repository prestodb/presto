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
package io.prestosql.plugin.phoenix;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.type.TypeManager;
import org.apache.phoenix.jdbc.PhoenixDriver;

import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PhoenixClientModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;
    private final TypeManager typeManager;

    public PhoenixClientModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = typeManager;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(PhoenixConnectorId.class).toInstance(new PhoenixConnectorId(connectorId));
        binder.bind(ConnectorSplitManager.class).to(PhoenixSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(PhoenixPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(PhoenixPageSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(PhoenixClient.class).in(Scopes.SINGLETON);
        binder.bind(PhoenixMetadataFactory.class).in(Scopes.SINGLETON);

        binder.bind(PhoenixSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(PhoenixTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).toInstance(typeManager);

        ensureCatalogIsEmpty(buildConfigObject(PhoenixConfig.class).getConnectionUrl());
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        try {
            PhoenixDriver driver = PhoenixDriver.INSTANCE;
            checkArgument(driver.acceptsURL(connectionUrl), "Invalid JDBC URL for Phoenix connector");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
