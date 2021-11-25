package com.facebook.presto.plugin.clickhouse;

import org.testcontainers.containers.ClickHouseContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testcontainers.containers.ClickHouseContainer.HTTP_PORT;

public class TestingClickHouseServer
        implements Closeable
{
    private static final String CLICKHOUSE_IMAGE = "yandex/clickhouse-server:20.8";
    private final ClickHouseContainer dockerContainer;

    public TestingClickHouseServer()
    {
        // Use 2nd stable version
        dockerContainer = (ClickHouseContainer) new ClickHouseContainer(CLICKHOUSE_IMAGE)
                .withStartupAttempts(10);

        dockerContainer.start();
    }

    public ClickHouseContainer getClickHouseContainer()
    {
        return dockerContainer;
    }
    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl()
    {
        String s = format("jdbc:clickhouse://%s:%s/", dockerContainer.getContainerIpAddress(),
                dockerContainer.getMappedPort(HTTP_PORT));
        return format("jdbc:clickhouse://%s:%s/", dockerContainer.getContainerIpAddress(),
                dockerContainer.getMappedPort(HTTP_PORT));
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }
}
