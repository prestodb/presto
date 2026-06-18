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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;

public class HiveAzureConfig
{
    private String wasbStorageAccount;
    private String wasbAccessKey;
    private String abfsStorageAccount;
    private String abfsAccessKey;
    private String abfsOAuthClientEndpoint;
    private String abfsOAuthClientId;
    private String abfsOAuthClientSecret;

    public Optional<String> getWasbStorageAccount()
    {
        return Optional.ofNullable(wasbStorageAccount);
    }

    @ConfigSecuritySensitive
    @Config("hive.azure.wasb-storage-account")
    public HiveAzureConfig setWasbStorageAccount(String wasbStorageAccount)
    {
        this.wasbStorageAccount = wasbStorageAccount;
        return this;
    }

    public Optional<String> getWasbAccessKey()
    {
        return Optional.ofNullable(wasbAccessKey);
    }

    @ConfigSecuritySensitive
    @Config("hive.azure.wasb-access-key")
    public HiveAzureConfig setWasbAccessKey(String wasbAccessKey)
    {
        this.wasbAccessKey = wasbAccessKey;
        return this;
    }

    public Optional<String> getAbfsStorageAccount()
    {
        return Optional.ofNullable(abfsStorageAccount);
    }

    @ConfigSecuritySensitive
    @Config("hive.azure.abfs-storage-account")
    public HiveAzureConfig setAbfsStorageAccount(String abfsStorageAccount)
    {
        this.abfsStorageAccount = abfsStorageAccount;
        return this;
    }

    public Optional<String> getAbfsAccessKey()
    {
        return Optional.ofNullable(abfsAccessKey);
    }

    @ConfigSecuritySensitive
    @Config("hive.azure.abfs-access-key")
    public HiveAzureConfig setAbfsAccessKey(String abfsAccessKey)
    {
        this.abfsAccessKey = abfsAccessKey;
        return this;
    }

    @ConfigSecuritySensitive
    @Config("hive.azure.abfs.oauth.endpoint")
    public HiveAzureConfig setAbfsOAuthClientEndpoint(String endpoint)
    {
        abfsOAuthClientEndpoint = endpoint;
        return this;
    }

    public Optional<String> getAbfsOAuthClientEndpoint()
    {
        return Optional.ofNullable(abfsOAuthClientEndpoint);
    }

    @ConfigSecuritySensitive
    @Config("hive.azure.abfs.oauth.client-id")
    public HiveAzureConfig setAbfsOAuthClientId(String id)
    {
        abfsOAuthClientId = id;
        return this;
    }

    public Optional<String> getAbfsOAuthClientId()
    {
        return Optional.ofNullable(abfsOAuthClientId);
    }

    @ConfigSecuritySensitive
    @Config("hive.azure.abfs.oauth.secret")
    public HiveAzureConfig setAbfsOAuthClientSecret(String secret)
    {
        abfsOAuthClientSecret = secret;
        return this;
    }

    public Optional<String> getAbfsOAuthClientSecret()
    {
        return Optional.ofNullable(abfsOAuthClientSecret);
    }
}
