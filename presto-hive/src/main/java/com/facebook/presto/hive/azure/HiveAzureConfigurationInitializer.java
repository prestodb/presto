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
package com.facebook.presto.hive.azure;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_ARRAY;

public class HiveAzureConfigurationInitializer
        implements AzureConfigurationInitializer
{
    private final Optional<String> wasbAccessKey;
    private final Optional<String> wasbStorageAccount;
    private final Optional<String> abfsAccessKey;
    private final Optional<String> abfsStorageAccount;
    private final Optional<String> abfsOAuthClientEndpoint;
    private final Optional<String> abfsOAuthClientId;
    private final Optional<String> abfsOAuthClientSecret;

    @Inject
    public HiveAzureConfigurationInitializer(HiveAzureConfig config)
    {
        this.wasbAccessKey = dropEmpty(config.getWasbAccessKey());
        this.wasbStorageAccount = dropEmpty(config.getWasbStorageAccount());
        if (wasbAccessKey.isPresent() || wasbStorageAccount.isPresent()) {
            checkArgument(
                    wasbAccessKey.isPresent() && wasbStorageAccount.isPresent(),
                    "If WASB storage account or access key is set, both must be set");
        }

        this.abfsAccessKey = dropEmpty(config.getAbfsAccessKey());
        this.abfsStorageAccount = dropEmpty(config.getAbfsStorageAccount());
        this.abfsOAuthClientEndpoint = dropEmpty(config.getAbfsOAuthClientEndpoint());
        this.abfsOAuthClientId = dropEmpty(config.getAbfsOAuthClientId());
        this.abfsOAuthClientSecret = dropEmpty(config.getAbfsOAuthClientSecret());
        if (abfsOAuthClientEndpoint.isPresent() || abfsOAuthClientSecret.isPresent() || abfsOAuthClientId.isPresent()) {
            checkArgument(
                    abfsOAuthClientEndpoint.isPresent() && abfsOAuthClientId.isPresent() && abfsOAuthClientSecret.isPresent(),
                    "If any of ABFS OAuth2 Client endpoint, ID, and secret are set, all must be set.");
        }
    }

    @Override
    public void updateConfiguration(Configuration config)
    {
        if (wasbAccessKey.isPresent() && wasbStorageAccount.isPresent()) {
            config.set(format("fs.azure.account.key.%s.blob.core.windows.net", wasbStorageAccount.get()), wasbAccessKey.get());
        }

        if (abfsAccessKey.isPresent() && abfsStorageAccount.isPresent()) {
            config.set(format("fs.azure.account.key.%s.dfs.core.windows.net", abfsStorageAccount.get()), abfsAccessKey.get());
            config.set("fs.abfs.impl", AzureBlobFileSystem.class.getName());
        }

        if (abfsOAuthClientEndpoint.isPresent() && abfsOAuthClientId.isPresent() && abfsOAuthClientSecret.isPresent()) {
            config.set(format("fs.azure.account.auth.type.%s.dfs.core.windows.net", abfsStorageAccount.get()), "OAuth");
            config.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
            config.set(format("fs.azure.account.oauth2.client.endpoint.%s.dfs.core.windows.net", abfsStorageAccount.get()), abfsOAuthClientEndpoint.get());
            config.set(format("fs.azure.account.oauth2.client.id.%s.dfs.core.windows.net", abfsStorageAccount.get()), abfsOAuthClientId.get());
            config.set(format("fs.azure.account.oauth2.client.secret.%s.dfs.core.windows.net", abfsStorageAccount.get()), abfsOAuthClientSecret.get());
        }

        // do not rely on information returned from local system about users and groups
        config.set("fs.azure.skipUserGroupMetadataDuringInitialization", "true");

        // disable buffering Azure output streams to disk(default is DATA_BLOCKS_BUFFER_DISK)
        config.set("fs.azure.data.blocks.buffer", DATA_BLOCKS_BUFFER_ARRAY);
    }

    private static Optional<String> dropEmpty(Optional<String> optional)
    {
        return optional.filter(value -> !value.isEmpty());
    }
    private static Proxy proxyForHost(HostAndPort address)
    {
        return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(address.getHost(), address.getPort()));
    }
}
