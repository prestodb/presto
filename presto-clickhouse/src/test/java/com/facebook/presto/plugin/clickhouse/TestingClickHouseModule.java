package com.facebook.presto.plugin.clickhouse;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import ru.yandex.clickhouse.ClickHouseDriver;

public class TestingClickHouseModule
        implements Module {

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(ClickHouseConfig.class);
    }

    @Provides
    public ClickHouseClient provideJdbcClient(ClickHouseConnectorId id, ClickHouseConfig config) {
        Properties connectionProperties = new Properties();
        return new ClickHouseClient(id, config, new DriverConnectionFactory(new ClickHouseDriver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties));
    }

    public static Map<String, String> createProperties() {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:clickhouse://localhost:8123/", System.nanoTime()))
                .build();
    }
}
