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

import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testcontainers.containers.ClickHouseContainer.HTTP_PORT;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingClickHouseServer
        implements Closeable
{
    private static final String CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.3.6.56";
    private final ClickHouseContainer dockerContainer;

    public TestingClickHouseServer()
    {
        this(DockerImageName.parse(CLICKHOUSE_IMAGE));
    }

    public TestingClickHouseServer(DockerImageName image)
    {
        // Use 2nd stable version
        dockerContainer = new ClickHouseContainer(image)
                .withCopyFileToContainer(forClasspathResource("custom.xml"), "/etc/clickhouse-server/config.d/custom.xml")
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
