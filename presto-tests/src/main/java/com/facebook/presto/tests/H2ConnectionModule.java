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
package com.facebook.presto.tests;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.functionNamespace.mysql.FunctionNamespaceDao;
import com.facebook.presto.functionNamespace.mysql.MySqlFunctionNamespaceManagerConfig;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.DriverManager;

import static com.facebook.presto.functionNamespace.mysql.MySqlConnectionModule.createJdbi;
import static com.google.inject.Scopes.SINGLETON;
import static java.lang.String.format;

public class H2ConnectionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        String databaseName = buildConfigObject(H2ConnectionConfig.class).getDatabaseName();
        Jdbi jdbi = createJdbi(
                () -> DriverManager.getConnection(getJdbcUrl(databaseName)),
                buildConfigObject(MySqlFunctionNamespaceManagerConfig.class));
        binder.bind(Jdbi.class).toInstance(jdbi);
        binder.bind(HandleLifecycleManager.class).in(SINGLETON);
        binder.bind(FunctionNamespaceDao.class).toProvider(new FunctionNamespaceDaoProvider()).in(SINGLETON);
        binder.bind(new TypeLiteral<Class<? extends FunctionNamespaceDao>>() {}).toInstance(H2FunctionNamespaceDao.class);
    }

    private static class HandleLifecycleManager
    {
        private final Handle handle;

        @Inject
        public HandleLifecycleManager(Jdbi jdbi)
        {
            this.handle = jdbi.open();
        }

        public Handle getHandle()
        {
            return handle;
        }

        @PreDestroy
        public void destroy()
        {
            handle.close();
        }
    }

    private static class FunctionNamespaceDaoProvider
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
            return injector.getInstance(HandleLifecycleManager.class).getHandle().attach(H2FunctionNamespaceDao.class);
        }
    }

    public static String getJdbcUrl(String databaseName)
    {
        return format("jdbc:h2:mem:test%s;MODE=MySQL;DATABASE_TO_LOWER=TRUE", databaseName);
    }
}
