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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.raptorx.util.Database.Type;
import com.facebook.presto.raptorx.util.SimpleDatabase;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import io.airlift.dbpool.MySqlDataSource;
import io.airlift.dbpool.MySqlDataSourceConfig;
import io.airlift.dbpool.PostgreSqlDataSource;
import io.airlift.dbpool.PostgreSqlDataSourceConfig;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.testing.StaticServiceSelector;

import javax.inject.Singleton;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;

public class DatabaseModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder ignored)
    {
        install(installModuleIf(
                DatabaseConfig.class,
                config -> "h2".equals(config.getDatabaseType()),
                binder -> binder.install(new H2DatabaseModule())));

        install(installModuleIf(
                DatabaseConfig.class,
                config -> "mysql".equals(config.getDatabaseType()),
                binder -> binder.install(new MySqlDatabaseModule())));

        install(installModuleIf(
                DatabaseConfig.class,
                config -> "postgresql".equals(config.getDatabaseType()),
                binder -> binder.install(new PostgreSqlDatabaseModule())));
    }

    private static class H2DatabaseModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(H2EmbeddedDataSourceConfig.class, "metadata");
            configBinder(binder).bindConfigDefaults(H2EmbeddedDataSourceConfig.class, config -> {
                config.setMaxConnections(100);
            });
        }

        @Provides
        @Singleton
        public static Database createDatabase(H2EmbeddedDataSourceConfig h2Config)
                throws Exception
        {
            DataSource dataSource = new H2EmbeddedDataSource(h2Config);
            return new SimpleDatabase(Type.H2, () -> getConnection(dataSource));
        }

        private static Connection getConnection(DataSource dataSource)
                throws SQLException
        {
            Connection connection = dataSource.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("SET MODE MySQL");
            }
            catch (SQLException e) {
                connection.close();
                throw e;
            }
            return connection;
        }
    }

    private static class MySqlDatabaseModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(JdbcDatabaseConfig.class);
            configBinder(binder).bindConfig(MySqlDataSourceConfig.class, "metadata");
            configBinder(binder).bindConfigDefaults(MySqlDataSourceConfig.class, config -> {
                config.setMaxConnections(100);
                config.setDefaultFetchSize(1000);
            });
        }

        @Provides
        @Singleton
        public static Database createDatabase(JdbcDatabaseConfig config, MySqlDataSourceConfig dataSourceConfig)
        {
            ServiceDescriptor descriptor = serviceDescriptor("mysql")
                    .addProperty("jdbc", config.getUrl())
                    .build();
            DataSource dataSource = new MySqlDataSource(new StaticServiceSelector(descriptor), dataSourceConfig);
            return new SimpleDatabase(Type.MYSQL, dataSource::getConnection);
        }
    }

    private static class PostgreSqlDatabaseModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(JdbcDatabaseConfig.class);
            configBinder(binder).bindConfig(PostgreSqlDataSourceConfig.class, "metadata");
            configBinder(binder).bindConfigDefaults(PostgreSqlDataSourceConfig.class, config -> {
                config.setMaxConnections(100);
                config.setDefaultFetchSize(1000);
            });
        }

        @Provides
        @Singleton
        public static Database createDatabase(JdbcDatabaseConfig config, PostgreSqlDataSourceConfig dataSourceConfig)
        {
            ServiceDescriptor descriptor = serviceDescriptor("postgresql")
                    .addProperty("jdbc", config.getUrl())
                    .build();
            DataSource dataSource = new PostgreSqlDataSource(new StaticServiceSelector(descriptor), dataSourceConfig);
            return new SimpleDatabase(Type.POSTGRESQL, dataSource::getConnection);
        }
    }
}
