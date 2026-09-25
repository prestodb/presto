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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.Network;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class S3MockContainer
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(S3MockContainer.class);

    // Adobe S3Mock (Apache 2.0) — S3-compatible mock; accepts any AWS credentials.
    public static final String DEFAULT_IMAGE = "adobe/s3mock:5.2.3";
    public static final String DEFAULT_HOST_NAME = "s3mock";

    public static final int S3_PORT = 4566;

    // S3Mock accepts any credentials; these are the canonical values for all test S3 clients.
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";

    public static Builder builder()
    {
        return new Builder();
    }

    private S3MockContainer(
            String image,
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
    protected void startContainer()
    {
        super.startContainer();
        log.info("S3Mock container started with address: http://%s", getApiEndpoint().toString());
    }

    public HostAndPort getApiEndpoint()
    {
        return getMappedHostAndPortForExposedPort(S3_PORT);
    }

    public static class Builder
            extends BaseTestContainer.Builder<S3MockContainer.Builder, S3MockContainer>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(S3_PORT);
            // COM_ADOBE_TESTING_S3MOCK_HTTP_PORT is the documented env var for S3Mock's plain-HTTP port.
            this.envVars = ImmutableMap.of("COM_ADOBE_TESTING_S3MOCK_HTTP_PORT", String.valueOf(S3_PORT));
        }

        @Override
        public Builder withEnvVars(Map<String, String> envVars)
        {
            Map<String, String> merged = new LinkedHashMap<>(envVars);
            merged.put("COM_ADOBE_TESTING_S3MOCK_HTTP_PORT", String.valueOf(S3_PORT));
            this.envVars = ImmutableMap.copyOf(merged);
            return this;
        }

        @Override
        public S3MockContainer build()
        {
            return new S3MockContainer(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
