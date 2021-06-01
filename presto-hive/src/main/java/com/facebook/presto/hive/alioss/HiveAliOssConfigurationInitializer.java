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
package com.facebook.presto.hive.alioss;

import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

public class HiveAliOssConfigurationInitializer
        implements AliOssConfigurationInitializer
{
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String endpoint;

    @Inject
    public HiveAliOssConfigurationInitializer(HiveAliOssConfig hiveAliOssConfig)
    {
        this.accessKeyId = hiveAliOssConfig.getAccessKeyId();
        this.accessKeySecret = hiveAliOssConfig.getAccessKeySecret();
        this.endpoint = hiveAliOssConfig.getEndpoint();
    }

    public void updateConfiguration(Configuration config)
    {
        // Only when user configured AliOss's information, we set them into the Hadoop configuration
        config.set(ALI_OSS_IMPL, PrestoAliOssFileSystem.class.getName());
        if (accessKeyId != null && accessKeySecret != null && endpoint != null) {
            config.set(ALI_OSS_ACCESS_KEY_ID, accessKeyId);
            config.set(ALI_OSS_ACCESS_KEY_SECRET, accessKeySecret);
            config.set(ALI_OSS_ENDPOINT, endpoint);
        }
    }
}
