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
package io.prestosql.plugin.kudu;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.MultibindingsScanner;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.prestosql.plugin.kudu.procedures.RangePartitionProcedures;
import io.prestosql.plugin.kudu.properties.KuduTableProperties;
import io.prestosql.plugin.kudu.schema.NoSchemaEmulation;
import io.prestosql.plugin.kudu.schema.SchemaEmulation;
import io.prestosql.plugin.kudu.schema.SchemaEmulationByTableNameConvention;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.TypeManager;
import org.apache.kudu.client.KuduClient;

import javax.inject.Singleton;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class KuduModule
        extends AbstractModule
{
    private final String catalogName;
    private final TypeManager typeManager;

    public KuduModule(String catalogName, TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName qis null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void configure()
    {
        install(MultibindingsScanner.asModule());

        bind(TypeManager.class).toInstance(typeManager);

        bind(KuduConnector.class).in(Scopes.SINGLETON);
        bind(KuduConnectorId.class).toInstance(new KuduConnectorId(catalogName));
        bind(KuduMetadata.class).in(Scopes.SINGLETON);
        bind(KuduTableProperties.class).in(Scopes.SINGLETON);
        bind(ConnectorSplitManager.class).to(KuduSplitManager.class).in(Scopes.SINGLETON);
        bind(ConnectorRecordSetProvider.class).to(KuduRecordSetProvider.class)
                .in(Scopes.SINGLETON);
        bind(ConnectorPageSourceProvider.class).to(KuduPageSourceProvider.class)
                .in(Scopes.SINGLETON);
        bind(ConnectorPageSinkProvider.class).to(KuduPageSinkProvider.class).in(Scopes.SINGLETON);
        bind(KuduHandleResolver.class).in(Scopes.SINGLETON);
        bind(KuduRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder()).bindConfig(KuduClientConfig.class);

        bind(RangePartitionProcedures.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder(), Procedure.class);
    }

    @ProvidesIntoSet
    Procedure getAddRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedures.getAddPartitionProcedure();
    }

    @ProvidesIntoSet
    Procedure getDropRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedures.getDropPartitionProcedure();
    }

    @Singleton
    @Provides
    KuduClientSession createKuduClientSession(
            KuduConnectorId connectorId,
            KuduClientConfig config)
    {
        requireNonNull(config, "config is null");

        KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(config.getMasterAddresses());
        builder.defaultAdminOperationTimeoutMs(config.getDefaultAdminOperationTimeout().toMillis());
        builder.defaultOperationTimeoutMs(config.getDefaultOperationTimeout().toMillis());
        builder.defaultSocketReadTimeoutMs(config.getDefaultSocketReadTimeout().toMillis());
        if (config.isDisableStatistics()) {
            builder.disableStatistics();
        }
        KuduClient client = builder.build();

        SchemaEmulation strategy;
        if (config.isSchemaEmulationEnabled()) {
            strategy = new SchemaEmulationByTableNameConvention(config.getSchemaEmulationPrefix());
        }
        else {
            strategy = new NoSchemaEmulation();
        }
        return new KuduClientSession(connectorId, client, strategy);
    }
}
