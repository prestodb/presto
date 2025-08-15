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
package com.facebook.presto.hive.gcs;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.AUTHENTICATION_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_ENABLE;
import static com.google.cloud.hadoop.util.AccessTokenProviderClassFromConfigFactory.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.cloud.hadoop.util.EntriesCredentialConfiguration.JSON_KEYFILE_SUFFIX;

public class HiveGcsConfigurationInitializer
        implements GcsConfigurationInitializer
{
    private final boolean useGcsAccessToken;
    private final String jsonKeyFilePath;

    @Inject
    public HiveGcsConfigurationInitializer(HiveGcsConfig config)
    {
        this.useGcsAccessToken = config.isUseGcsAccessToken();
        this.jsonKeyFilePath = config.getJsonKeyFilePath();
    }

    public void updateConfiguration(Configuration config)
    {
        config.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());

        if (useGcsAccessToken) {
            // use oauth token to authenticate with Google Cloud Storage
            config.set(AUTH_SERVICE_ACCOUNT_ENABLE.getKey(), "false");
            config.set(AUTHENTICATION_PREFIX + ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX, GcsAccessTokenProvider.class.getName());
        }
        else if (jsonKeyFilePath != null) {
            // use service account key file
            config.set(AUTH_SERVICE_ACCOUNT_ENABLE.getKey(), "true");
            config.set(AUTHENTICATION_PREFIX + JSON_KEYFILE_SUFFIX, jsonKeyFilePath);
        }
    }
}
