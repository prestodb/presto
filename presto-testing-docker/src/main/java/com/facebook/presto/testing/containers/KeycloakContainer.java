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
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.RealmRepresentation;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KeycloakContainer
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(KeycloakContainer.class);

    public static final String DEFAULT_IMAGE = "quay.io/keycloak/keycloak:26.4.2";
    public static final String DEFAULT_HOST_NAME = "keycloak";

    public static final String DEFAULT_USER_NAME = "admin";
    public static final String DEFAULT_PASSWORD = "admin";

    public static final String MASTER_REALM = "master";
    public static final String ADMIN_CLI_CLIENT = "admin-cli";

    public static final int PORT = 8080;
    public static final String SERVER_URL = "http://" + DEFAULT_HOST_NAME + ":" + PORT;

    public static Builder builder()
    {
        return new Builder();
    }

    protected KeycloakContainer(String image,
                                String hostName,
                                Set<Integer> exposePorts,
                                Map<String, String> filesToMount,
                                Map<String, String> envVars,
                                Optional<Network> network,
                                int retryLimit)
    {
        super(
                image,
                hostName,
                exposePorts,
                filesToMount,
                envVars,
                network,
                retryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(ImmutableList.of("start-dev"));
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Keycloak container started with URL: %s", getUrl());
    }

    public String getUrl()
    {
        return "http://" + getMappedHostAndPortForExposedPort(PORT);
    }

    public String getAccessToken()
    {
        try (Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(getUrl())
                .realm(MASTER_REALM)
                .clientId(ADMIN_CLI_CLIENT)
                .username(DEFAULT_USER_NAME)
                .password(DEFAULT_PASSWORD)
                .build()) {
            RealmResource master = keycloak.realm(MASTER_REALM);
            RealmRepresentation masterRep = master.toRepresentation();
            // change access token lifespan from 1 minute (default) to 1 hour
            // to keep the token alive in case testcase takes more than a minute to finish execution.
            masterRep.setAccessTokenLifespan(3600);
            master.update(masterRep);
            return keycloak.tokenManager().getAccessTokenString();
        }
    }

    public static class Builder
            extends BaseTestContainer.Builder<KeycloakContainer.Builder, KeycloakContainer>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(PORT);
            this.envVars = ImmutableMap.of(
                    "KC_BOOTSTRAP_ADMIN_USERNAME", DEFAULT_USER_NAME,
                    "KC_BOOTSTRAP_ADMIN_PASSWORD", DEFAULT_PASSWORD,
                    "KC_HOSTNAME", SERVER_URL);
        }

        @Override
        public KeycloakContainer build()
        {
            return new KeycloakContainer(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
