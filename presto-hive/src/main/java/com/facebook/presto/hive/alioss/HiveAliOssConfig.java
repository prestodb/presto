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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class HiveAliOssConfig
{
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;

    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    @Config("hive.alioss.access-key-id")
    @ConfigDescription("AccessKeyId used to access Alibaba Cloud OSS")
    public HiveAliOssConfig setAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("hive.alioss.endpoint")
    @ConfigDescription("Endpoint used to access Alibaba Cloud OSS")
    public HiveAliOssConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getAccessKeySecret()
    {
        return accessKeySecret;
    }

    @Config("hive.alioss.access-key-secret")
    @ConfigDescription("AccessKeySecret used to access Alibaba Cloud OSS")
    public HiveAliOssConfig setAccessKeySecret(String accessKeySecret)
    {
        this.accessKeySecret = accessKeySecret;
        return this;
    }
}
