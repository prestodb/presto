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
package com.facebook.presto.plugin.mysql;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.mysql.jdbc.Driver;

import java.sql.SQLException;
import java.util.Properties;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MySqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(MySqlClient.class).in(Scopes.SINGLETON);
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
        configBinder(binder).bindConfig(MySqlConfig.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        try {
            Driver driver = new Driver();
            Properties urlProperties = driver.parseURL(connectionUrl, null);
            requireNonNull(urlProperties, "Invalid JDBC URL for MySQL connector");
            checkArgument(driver.database(urlProperties) == null, "Database (catalog) must not be specified in JDBC URL for MySQL connector");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
