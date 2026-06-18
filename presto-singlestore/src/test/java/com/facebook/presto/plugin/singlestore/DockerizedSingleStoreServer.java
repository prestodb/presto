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
    private static final String SINGLESTORE_LICENSE_PROPERTY = "SINGLESTORE_LICENSE";

    private final SingleStoreContainer dockerContainer;

    public DockerizedSingleStoreServer()
    {
        String license = System.getenv(SINGLESTORE_LICENSE_PROPERTY);
        if (license == null || license.isEmpty()) {
            LOG.info("Using default license value, environment variable: '{}' is missed.", SINGLESTORE_LICENSE_PROPERTY);
            license = "BGNkNGNlYzFlYWYwYjQ0Yjc5ZGQ5ZTUwM2NjZTc3OWMxAAAAAAAAAAAEAAAAAAAAACgwNQIYK8UIk2TShKVDLkN3bRKbH/JMFGURGStoAhkAtCoka/omJzS5DWPltpVhJMjqh4ZV2bYEAA==";
        }
        assertTrue(license != null && !license.isEmpty(), "Missed environment variable: '" + SINGLESTORE_LICENSE_PROPERTY + "'");
        this.dockerContainer = new SingleStoreContainer(license);
        this.dockerContainer.start();
    }

    public void setGlobalVariable(String name, String value)
            throws SQLException
    {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl()); Statement stmt = conn.createStatement()) {
            stmt.execute(format("SET GLOBAL %s = %s", name, value));
        }
    }

    public String getJdbcUrl()
    {
        return dockerContainer.getJdbcUrl();
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
