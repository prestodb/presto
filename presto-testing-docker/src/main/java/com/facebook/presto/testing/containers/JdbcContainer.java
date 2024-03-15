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
package com.facebook.presto.testing.containers;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class JdbcContainer
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(JdbcContainer.class);

    public static final String DEFAULT_IMAGE = "postgres";
    public static final String DEFAULT_HOST_NAME = "postgres";
    public static final String DEFAULT_USER = "postgres";
    public static final String DEFAULT_PASSWORD = "password";
    public static final String DEFAULT_DRIVER_CLASS = "org.postgresql.Driver";
    public static final int PORT = 5432;

    public static Builder builder()
    {
        return new Builder();
    }

    private JdbcContainer(String image, String hostName, Set<Integer> exposePorts, Map<String, String> filesToMount, Map<String, String> envVars, Optional<Network> network, int retryLimit)
    {
        super(image, hostName, exposePorts, filesToMount, envVars, network, retryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(ImmutableList.of("sh", "/postgresScript.sh"));
    }

    @Override
    protected void startContainer()
    {
        super.startContainer();
        log.info("Postgres server container started with address: %s", getMappedHostAndPortForExposedPort(PORT));
    }

    public String getJdbcURI()
    {
        return "jdbc:postgresql://" + getMappedHostAndPortForExposedPort(PORT) + "/";
    }

    public String getDefaultUser()
    {
        return DEFAULT_USER;
    }

    public String getDefaultPassword()
    {
        return DEFAULT_PASSWORD;
    }

    public String getDefaultDriverClass()
    {
        return DEFAULT_DRIVER_CLASS;
    }

    public static class Builder
            extends BaseTestContainer.Builder<JdbcContainer.Builder, JdbcContainer>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(PORT);
            this.envVars = ImmutableMap.of("POSTGRES_USER", DEFAULT_USER,
                    "POSTGRES_PASSWORD", DEFAULT_PASSWORD);
        }

        @Override
        public JdbcContainer build()
        {
            return new JdbcContainer(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
