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

package com.facebook.presto.plugin.singlestore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.testing.docker.DockerContainer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class DockerizedSingleStoreServer
        implements Closeable
{
    private static final Logger LOG = Logger.get(DockerizedSingleStoreServer.class);
    private static final int SINGLESTORE_PORT = 3306;
    private static final String SINGLESTORE_USER = "root";
    private static final String ROOT_PASSWORD = "LbRootPass1";
    private static final String SINGLESTORE_LICENSE_PROPERTY = "SINGLESTORE_LICENSE";

    private final DockerContainer dockerContainer;

    public DockerizedSingleStoreServer()
    {
        String license = System.getenv(SINGLESTORE_LICENSE_PROPERTY);
        if (license == null || license.isEmpty()) {
            LOG.info("Missed environment variable: '" + SINGLESTORE_LICENSE_PROPERTY + "'");
            license = "BGNkNGNlYzFlYWYwYjQ0Yjc5ZGQ5ZTUwM2NjZTc3OWMxAAAAAAAAAAAEAAAAAAAAACgwNQIYK8UIk2TShKVDLkN3bRKbH/JMFGURGStoAhkAtCoka/omJzS5DWPltpVhJMjqh4ZV2bYEAA==";
        }
        assertTrue(license != null && !license.isEmpty(), "Missed environment variable: '" + SINGLESTORE_LICENSE_PROPERTY + "'");
        this.dockerContainer = new DockerContainer(
                "ghcr.io/singlestore-labs/singlestoredb-dev:latest",
                ImmutableList.of(SINGLESTORE_PORT),
                ImmutableMap.of(
                        "SINGLESTORE_LICENSE", license,
                        "ROOT_PASSWORD", ROOT_PASSWORD),
                DockerizedSingleStoreServer::healthCheck);
    }

    private static void healthCheck(DockerContainer.HostPortProvider hostPortProvider)
            throws SQLException
    {
        String jdbcUrl = getJdbcUrl(hostPortProvider, SINGLESTORE_USER, ROOT_PASSWORD);
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
        }
    }

    public String getJdbcUrl()
    {
        return getJdbcUrl(dockerContainer::getHostPort, SINGLESTORE_USER, ROOT_PASSWORD);
    }

    private static String getJdbcUrl(DockerContainer.HostPortProvider hostPortProvider, String user, String password)
    {
        return format("jdbc:singlestore://localhost:%s?user=%s&password=%s", hostPortProvider.getHostPort(SINGLESTORE_PORT), user, password);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
