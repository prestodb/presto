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
package com.facebook.presto.hive.containers;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.testing.containers.BaseTestContainer;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

public class HiveHadoopContainer
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(HiveHadoopContainer.class);

    private static final String IMAGE_VERSION = "11";
    public static final String DEFAULT_IMAGE = "prestodb/hdp3.1-hive:" + IMAGE_VERSION;
    public static final String HIVE3_IMAGE = "prestodb/hive3.1-hive:10";

    public static final String HOST_NAME = "hadoop-master";

    public static final int PROXY_PORT = 1180;
    public static final int HIVE_METASTORE_PORT = 9083;

    public static Builder builder()
    {
        return new Builder();
    }

    private HiveHadoopContainer(
            String image,
            String hostName,
            Set<Integer> ports,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int startupRetryLimit)
    {
        super(
                image,
                hostName,
                ports,
                filesToMount,
                envVars,
                network,
                startupRetryLimit);
    }

    @Override
    protected void startContainer()
    {
        super.startContainer();
        log.info(format(
                "HiveHadoop container started with address, HiveMetastore: http://%s and SocksProxy: http://%s",
                getHiveMetastoreEndpoint().toString(),
                getMappedHdfsSocksProxy().toString()));
    }

    public HostAndPort getMappedHdfsSocksProxy()
    {
        return getMappedHostAndPortForExposedPort(PROXY_PORT);
    }

    public HostAndPort getHiveMetastoreEndpoint()
    {
        return getMappedHostAndPortForExposedPort(HIVE_METASTORE_PORT);
    }

    public static class Builder
            extends BaseTestContainer.Builder<HiveHadoopContainer.Builder, HiveHadoopContainer>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = HOST_NAME;
            this.exposePorts =
                    ImmutableSet.of(
                            HIVE_METASTORE_PORT,
                            PROXY_PORT);
        }

        @Override
        public HiveHadoopContainer build()
        {
            return new HiveHadoopContainer(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
