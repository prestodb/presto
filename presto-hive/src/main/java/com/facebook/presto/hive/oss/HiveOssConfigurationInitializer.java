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
package com.facebook.presto.hive.oss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HiveOssConfigurationInitializer
        implements OssConfigurationInitializer
{
    private final HiveOssConfig hiveOssConfig;

    @Inject
    public HiveOssConfigurationInitializer(HiveOssConfig hiveOssConfig)
    {
        this.hiveOssConfig = requireNonNull(hiveOssConfig, "hiveOssConfig is null");
    }

    public void updateConfiguration(Configuration config)
    {
        // Only when user configured oss's information, we will set them into the Hadoop configuration
        if (hiveOssConfig.getAccessKeyId() != null
                && hiveOssConfig.getAccessKeySecret() != null
                && hiveOssConfig.getEndpoint() != null) {
            config.set("fs.oss.impl", AliyunOSSFileSystem.class.getName());
            config.set("fs.oss.accessKeyId", hiveOssConfig.getAccessKeyId());
            config.set("fs.oss.accessKeySecret", hiveOssConfig.getAccessKeySecret());
            config.set("fs.oss.endpoint", hiveOssConfig.getEndpoint());
        }
    }
}
