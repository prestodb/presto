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

import com.facebook.presto.testing.docker.DockerContainer;
import com.facebook.presto.testing.docker.DockerContainer.HostPortProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class DockerizedMySqlServer
        implements Closeable
{
    private static final int MYSQL_PORT = 3306;

    private static final String MYSQL_ROOT_USER = "root";
    private static final String MYSQL_ROOT_PASSWORD = "mysqlrootpassword";
    private static final String MYSQL_USER = "testuser";
    private static final String MYSQL_PASSWORD = "testpassword";

    private final DockerContainer dockerContainer;

    public DockerizedMySqlServer()
    {
        this.dockerContainer = new DockerContainer(
                "mysql:8.0.15",
                ImmutableList.of(MYSQL_PORT),
                ImmutableMap.of(
                        "MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD,
                        "MYSQL_USER", MYSQL_USER,
                        "MYSQL_PASSWORD", MYSQL_PASSWORD,
                        "MYSQL_DATABASE", "tpch"),
                DockerizedMySqlServer::healthCheck);
    }

    private static void healthCheck(HostPortProvider hostPortProvider)
            throws SQLException
    {
        String jdbcUrl = getJdbcUrl(hostPortProvider, MYSQL_ROOT_USER, MYSQL_ROOT_PASSWORD);
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
        }
    }

    public void init()
            throws SQLException
    {
        String jdbcUrl = getJdbcUrl(dockerContainer::getHostPort, MYSQL_ROOT_USER, MYSQL_ROOT_PASSWORD);
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            stmt.execute(format("GRANT ALL PRIVILEGES ON *.* TO '%s'", MYSQL_USER));
        }
    }

    public String getJdbcUrl()
    {
        return getJdbcUrl(dockerContainer::getHostPort, MYSQL_USER, MYSQL_PASSWORD);
    }

    private static String getJdbcUrl(HostPortProvider hostPortProvider, String user, String password)
    {
        return format("jdbc:mysql://localhost:%s?user=%s&password=%s&useSSL=false&allowPublicKeyRetrieval=true", hostPortProvider.getHostPort(MYSQL_PORT), user, password);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
