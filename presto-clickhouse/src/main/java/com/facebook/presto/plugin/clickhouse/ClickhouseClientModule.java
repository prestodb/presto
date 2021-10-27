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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.airlift.configuration.ConfigBinder;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.Asserts;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

public class ClickhouseClientModule
        implements Module
{
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).to(ClickhouseClient.class).in(Scopes.SINGLETON);
        ConfigBinder.configBinder(binder).bindConfig(BaseJdbcConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(ClickhouseConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(HttpPost.class);
    }

    @Provides
    @Singleton
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, ClickhouseConfig clickhouseConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        Asserts.notEmpty(config.getConnectionUrl(), "connection-url");
        return (ConnectionFactory) new DriverConnectionFactory((Driver) new ClickHouseDriver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }
}
