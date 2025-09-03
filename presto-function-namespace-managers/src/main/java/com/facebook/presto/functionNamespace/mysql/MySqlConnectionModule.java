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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import jakarta.inject.Inject;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Provider;

import java.sql.DriverManager;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class MySqlConnectionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MySqlConnectionConfig.class);

        MySqlConnectionConfig mySqlConnectionConfig = buildConfigObject(MySqlConnectionConfig.class);
        String databaseUrl = mySqlConnectionConfig.getDatabaseUrl();
        String jdbcDriverName = mySqlConnectionConfig.getJdbcDriverName();
        try {
            Class.forName(jdbcDriverName);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Jdbi jdbi = createJdbi(
                () -> DriverManager.getConnection(databaseUrl),
                buildConfigObject(MySqlFunctionNamespaceManagerConfig.class));
        binder.bind(Jdbi.class).toInstance(jdbi);
        binder.bind(FunctionNamespaceDao.class).toProvider(FunctionNamespaceDaoProvider.class);
        binder.bind(new TypeLiteral<Class<? extends FunctionNamespaceDao>>() {}).toInstance(FunctionNamespaceDao.class);
    }

    public static class FunctionNamespaceDaoProvider
            implements Provider<FunctionNamespaceDao>
    {
        private Injector injector;

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public FunctionNamespaceDao get()
        {
            return injector.getInstance(Jdbi.class).onDemand(FunctionNamespaceDao.class);
        }
    }

    public static Jdbi createJdbi(String url, MySqlFunctionNamespaceManagerConfig config)
    {
        return createJdbi(() -> DriverManager.getConnection(url), config);
    }

    public static Jdbi createJdbi(ConnectionFactory connectionFactory, MySqlFunctionNamespaceManagerConfig config)
    {
        Jdbi jdbi = Jdbi.create(connectionFactory).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(FunctionNamespacesTableCustomizerFactory.Config.class)
                .setTableName(config.getFunctionNamespacesTableName());
        jdbi.getConfig(SqlFunctionsTableCustomizerFactory.Config.class)
                .setTableName(config.getFunctionsTableName());
        jdbi.getConfig(UserDefinedTypesTableCustomizerFactory.Config.class)
                .setTableName(config.getUserDefinedTypesTableName());
        return jdbi;
    }
}
