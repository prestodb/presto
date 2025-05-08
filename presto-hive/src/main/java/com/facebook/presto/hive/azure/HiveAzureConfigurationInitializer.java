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

import com.facebook.airlift.log.Logger;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_ARRAY;

public class HiveAzureConfigurationInitializer
        implements AzureConfigurationInitializer
{
    private static final Logger log = Logger.get(HiveAzureConfigurationInitializer.class);

    // Azure endpoint suffixes
    private static final String BLOB_ENDPOINT_SUFFIX = ".blob.core.windows.net";
    private static final String DFS_ENDPOINT_SUFFIX = ".dfs.core.windows.net";

    // WASB uses legacy property format (not in ConfigurationKeys)
    private static final String WASB_ACCOUNT_KEY_PROPERTY_FORMAT = "fs.azure.account.key.%s" + BLOB_ENDPOINT_SUFFIX;

    private static final String ABFS_IMPL_PROPERTY = "fs.abfs.impl";
    private static final String OAUTH_AUTH_TYPE = "OAuth";
    private static final String ABFS_OAUTH_PROVIDER = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";

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
        checkArgument(
                wasbAccessKey.isPresent() == wasbStorageAccount.isPresent(),
                "If WASB storage account or access key is set, both must be set");

        this.abfsAccessKey = dropEmpty(config.getAbfsAccessKey());
        this.abfsStorageAccount = dropEmpty(config.getAbfsStorageAccount());
        this.abfsOAuthClientEndpoint = dropEmpty(config.getAbfsOAuthClientEndpoint());
        this.abfsOAuthClientId = dropEmpty(config.getAbfsOAuthClientId());
        this.abfsOAuthClientSecret = dropEmpty(config.getAbfsOAuthClientSecret());

        boolean hasAbfsAccessKey = abfsAccessKey.isPresent();
        boolean hasAbfsOAuth = abfsOAuthClientEndpoint.isPresent() || abfsOAuthClientId.isPresent() || abfsOAuthClientSecret.isPresent();
        boolean hasAbfsStorageAccount = abfsStorageAccount.isPresent();

        if (hasAbfsAccessKey || hasAbfsOAuth || hasAbfsStorageAccount) {
            checkArgument(
                    hasAbfsStorageAccount && (hasAbfsAccessKey || hasAbfsOAuth),
                    "ABFS requires both storage account and an authentication method (access key or OAuth)");

            if (hasAbfsOAuth) {
                checkArgument(
                        abfsOAuthClientEndpoint.isPresent() && abfsOAuthClientId.isPresent() && abfsOAuthClientSecret.isPresent(),
                        "ABFS OAuth2 authentication requires endpoint, client ID, and client secret, all must be set");
                if (hasAbfsAccessKey) {
                    log.warn("Both ABFS OAuth2 and AccessKey are configured. OAuth2 takes precedence with no fallback.");
                }
            }
        }
    }

    @Override
    public void updateConfiguration(Configuration config)
    {
        if (wasbAccessKey.isPresent() && wasbStorageAccount.isPresent()) {
            config.set(format(WASB_ACCOUNT_KEY_PROPERTY_FORMAT, wasbStorageAccount.get()), wasbAccessKey.get());
        }

        if (abfsStorageAccount.isPresent()) {
            String dfsEndpoint = abfsStorageAccount.get() + DFS_ENDPOINT_SUFFIX;

            if (abfsAccessKey.isPresent()) {
                config.set(ConfigurationKeys.accountProperty(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, dfsEndpoint), abfsAccessKey.get());
                if (config.get(ABFS_IMPL_PROPERTY) == null) {
                    config.set(ABFS_IMPL_PROPERTY, AzureBlobFileSystem.class.getName());
                }
            }

            if (abfsOAuthClientEndpoint.isPresent() && abfsOAuthClientId.isPresent() && abfsOAuthClientSecret.isPresent()) {
                config.set(ConfigurationKeys.accountProperty(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, dfsEndpoint), OAUTH_AUTH_TYPE);
                config.set(ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, ABFS_OAUTH_PROVIDER);
                config.set(ConfigurationKeys.accountProperty(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT, dfsEndpoint), abfsOAuthClientEndpoint.get());
                config.set(ConfigurationKeys.accountProperty(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID, dfsEndpoint), abfsOAuthClientId.get());
                config.set(ConfigurationKeys.accountProperty(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET, dfsEndpoint), abfsOAuthClientSecret.get());
            }
        }

        // do not rely on information returned from local system about users and groups
        config.set(ConfigurationKeys.AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION, "true");

        // disable buffering Azure output streams to disk(default is DATA_BLOCKS_BUFFER_DISK)
        config.set(ConfigurationKeys.DATA_BLOCKS_BUFFER, DATA_BLOCKS_BUFFER_ARRAY);
    }

    private static Optional<String> dropEmpty(Optional<String> optional)
    {
        return optional.filter(value -> !value.isEmpty());
    }
}
