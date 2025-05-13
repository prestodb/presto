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
package com.facebook.presto.hive.aws.security;

import static java.util.Objects.requireNonNull;

public class BasicAWSCredentials
{
    private final String accessKey;
    private final String secretKey;

    public BasicAWSCredentials(String accessKey, String secretKey)
    {
        this.accessKey = requireNonNull(accessKey, "Access key cannot be null");
        this.secretKey = requireNonNull(secretKey, "Secret key cannot be null");
    }

    public String getAWSAccessKeyId()
    {
        return this.accessKey;
    }

    public String getAWSSecretKey()
    {
        return this.secretKey;
    }
}
