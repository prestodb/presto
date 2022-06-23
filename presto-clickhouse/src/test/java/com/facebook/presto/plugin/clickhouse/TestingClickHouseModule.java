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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

public class TestingClickHouseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ClickHouseConfig.class);
    }

    @Provides
    public ClickHouseClient provideJdbcClient(ClickHouseConnectorId id, ClickHouseConfig config)
    {
        Properties connectionProperties = new Properties();
        return new ClickHouseClient(id, config, new DriverConnectionFactory(new ClickHouseDriver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredential()),
                Optional.ofNullable(config.getPasswordCredential()),
                connectionProperties));
    }

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("clickhouse.connection-url", format("jdbc:clickhouse://localhost:8123/", System.nanoTime()))
                .build();
    }
}
