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
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.sql.DriverManager;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class MySqlConnectionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MySqlConnectionConfig.class);

        String databaseUrl = buildConfigObject(MySqlConnectionConfig.class).getDatabaseUrl();
        binder.bind(Jdbi.class).toInstance(Jdbi.create(() -> DriverManager.getConnection(databaseUrl)).installPlugin(new SqlObjectPlugin()));
    }
}
