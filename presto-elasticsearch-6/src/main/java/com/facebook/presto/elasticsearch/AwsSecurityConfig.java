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
package com.facebook.presto.elasticsearch;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class AwsSecurityConfig
{
    private String accessKey;
    private String secretKey;
    private boolean useAwsInstanceCredentials;
    private String region;

    @NotNull
    public Optional<String> getAccessKey()
    {
        return Optional.ofNullable(accessKey);
    }

    @Config("elasticsearch.aws.access-key")
    public AwsSecurityConfig setAccessKey(String key)
    {
        this.accessKey = key;
        return this;
    }

    @NotNull
    public Optional<String> getSecretKey()
    {
        return Optional.ofNullable(secretKey);
    }

    @Config("elasticsearch.aws.secret-key")
    public AwsSecurityConfig setSecretKey(String key)
    {
        this.secretKey = key;
        return this;
    }

    public boolean isUseInstanceCredentials()
    {
        return useAwsInstanceCredentials;
    }

    @Config("elasticsearch.aws.use-instance-credentials")
    public AwsSecurityConfig setUseInstanceCredentials(boolean use)
    {
        this.useAwsInstanceCredentials = use;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("elasticsearch.aws.region")
    public AwsSecurityConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }
}
