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

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

public class SingleStoreContainer
        extends JdbcDatabaseContainer<SingleStoreContainer>
{
    private static final String DEFAULT_DOCKER_IMAGE_NAME = "ghcr.io/singlestore-labs/singlestoredb-dev";
    private static final String DEFAULT_DOCKER_IMAGE_TAG = "0.2.24";
    private static final String DEFAULT_ROOT_PASSWORD = "root";
    private static final String DEFAULT_ROOT = "root";
    private static final int PORT = 3306;

    private final String license;
    private final String rootPassword;

    public SingleStoreContainer(String license)
    {
        this(DEFAULT_DOCKER_IMAGE_NAME, DEFAULT_DOCKER_IMAGE_TAG, license, DEFAULT_ROOT_PASSWORD);
    }

    public SingleStoreContainer(String license, String rootPassword)
    {
        this(DEFAULT_DOCKER_IMAGE_NAME, DEFAULT_DOCKER_IMAGE_TAG, license, rootPassword);
    }

    public SingleStoreContainer(String dockerImageName, String dockerImageTag, String license, String rootPassword)
    {
        super(DockerImageName.parse(dockerImageName).withTag(dockerImageTag));
        this.license = license;
        this.rootPassword = rootPassword;
        this.preconfigure();
    }

    private void preconfigure()
    {
        this.withStartupTimeoutSeconds(240);
        this.withConnectTimeoutSeconds(120);
        this.addEnv("SINGLESTORE_LICENSE", license);
        this.addEnv("ROOT_PASSWORD", rootPassword);
        this.addExposedPorts(PORT);
    }

    @Override
    public String getDriverClassName()
    {
        return "com.singlestore.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl()
    {
        return String.format("jdbc:singlestore://localhost:%s?user=%s&password=%s", getMappedPort(PORT), DEFAULT_ROOT, rootPassword);
    }

    @Override
    public String getUsername()
    {
        return DEFAULT_ROOT;
    }

    @Override
    public String getPassword()
    {
        return rootPassword;
    }

    @Override
    protected String getTestQueryString()
    {
        return "SELECT 1";
    }
}
