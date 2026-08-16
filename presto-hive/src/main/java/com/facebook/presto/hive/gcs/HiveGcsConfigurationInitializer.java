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
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;

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
            config.set(GCS_CONFIG_PREFIX + ENABLE_SERVICE_ACCOUNTS_SUFFIX.getKey(), "false");
            config.set(GCS_CONFIG_PREFIX + ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX.getKey(), GcsAccessTokenProvider.class.getName());
        }
        else if (jsonKeyFilePath != null) {
            // use service account key file
            config.set(GCS_CONFIG_PREFIX + ENABLE_SERVICE_ACCOUNTS_SUFFIX.getKey(), "true");
            config.set(GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), jsonKeyFilePath);
        }
    }
}
