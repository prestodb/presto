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
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

public class MinIOContainer
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(MinIOContainer.class);

    public static final String DEFAULT_IMAGE = "minio/minio:RELEASE.2021-07-15T22-27-34Z";
    public static final String DEFAULT_HOST_NAME = "minio";

    public static final int MINIO_API_PORT = 4566;
    public static final int MINIO_CONSOLE_PORT = 4567;

    public static Builder builder()
    {
        return new Builder();
    }

    private MinIOContainer(
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
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(
                ImmutableList.of(
                        "server",
                        "--address", "0.0.0.0:" + MINIO_API_PORT,
                        "--console-address", "0.0.0.0:" + MINIO_CONSOLE_PORT,
                        "/data"));
    }

    @Override
    protected void startContainer()
    {
        super.startContainer();
        log.info(format(
                "MinIO container started with address for api: http://%s and console: http://%s",
                getMinioApiEndpoint().toString(),
                getMinioConsoleEndpoint().toString()));
    }

    public HostAndPort getMinioApiEndpoint()
    {
        return getMappedHostAndPortForExposedPort(MINIO_API_PORT);
    }

    public HostAndPort getMinioConsoleEndpoint()
    {
        return getMappedHostAndPortForExposedPort(MINIO_CONSOLE_PORT);
    }

    public static class Builder
            extends BaseTestContainer.Builder<MinIOContainer.Builder, MinIOContainer>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts =
                    ImmutableSet.of(
                            MINIO_API_PORT,
                            MINIO_CONSOLE_PORT);
        }

        @Override
        public MinIOContainer build()
        {
            return new MinIOContainer(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
