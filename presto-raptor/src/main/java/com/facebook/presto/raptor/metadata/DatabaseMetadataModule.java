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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.util.DaoSupplier;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import io.airlift.dbpool.MySqlDataSource;
import io.airlift.dbpool.MySqlDataSourceConfig;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.sql.DataSource;

import java.lang.reflect.Type;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static java.util.Objects.requireNonNull;

public class DatabaseMetadataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder ignored)
    {
        install(installModuleIf(
                DatabaseConfig.class,
                config -> "mysql".equals(config.getDatabaseType()),
                binder -> {
                    binder.install(new MySqlDataSourceModule());
                    bindDaoSupplier(binder, ShardDao.class, MySqlShardDao.class);
                }));

        install(installModuleIf(
                DatabaseConfig.class,
                config -> "h2".equals(config.getDatabaseType()),
                binder -> {
                    binder.install(new H2EmbeddedDataSourceModule("metadata", ForMetadata.class));
                    bindDaoSupplier(binder, ShardDao.class, H2ShardDao.class);
                }));
    }

    @ForMetadata
    @Singleton
    @Provides
    public ConnectionFactory createConnectionFactory(@ForMetadata DataSource dataSource)
    {
        return dataSource::getConnection;
    }

    public static <B, T extends B> void bindDaoSupplier(Binder binder, Class<B> baseType, Class<T> type)
    {
        binder.bind(daoSupplierTypeToken(baseType))
                .toProvider(new DaoSupplierProvider<>(type))
                .in(Scopes.SINGLETON);
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeLiteral<DaoSupplier<? extends T>> daoSupplierTypeToken(Class<T> type)
    {
        Type javaType = new TypeToken<DaoSupplier<T>>() {}
                .where(new TypeParameter<T>() {}, TypeToken.of(type))
                .getType();
        return (TypeLiteral<DaoSupplier<? extends T>>) TypeLiteral.get(javaType);
    }

    private static class DaoSupplierProvider<T>
            implements Provider<DaoSupplier<T>>
    {
        private final Class<T> type;
        private Injector injector;

        public DaoSupplierProvider(Class<T> type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public DaoSupplier<T> get()
        {
            checkState(injector != null, "injector was not set");
            IDBI dbi = injector.getInstance(Key.get(IDBI.class, ForMetadata.class));
            return new DaoSupplier<>(dbi, type);
        }
    }

    private static class MySqlDataSourceModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(JdbcDatabaseConfig.class);
            configBinder(binder).bindConfig(MySqlDataSourceConfig.class, ForMetadata.class, "metadata");
            configBinder(binder).bindConfigDefaults(MySqlDataSourceConfig.class, ForMetadata.class, config -> {
                config.setMaxConnections(100);
                config.setDefaultFetchSize(1000);
            });
        }

        @ForMetadata
        @Singleton
        @Provides
        DataSource createDataSource(JdbcDatabaseConfig config, @ForMetadata MySqlDataSourceConfig mysqlConfig)
        {
            ServiceDescriptor descriptor = serviceDescriptor("mysql")
                    .addProperty("jdbc", config.getUrl())
                    .build();
            return new MySqlDataSource(new StaticServiceSelector(descriptor), mysqlConfig);
        }
    }
}
