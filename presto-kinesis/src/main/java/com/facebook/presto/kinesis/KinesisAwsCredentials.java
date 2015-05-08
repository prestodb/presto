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
package com.facebook.presto.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.google.inject.Inject;

public class KinesisAwsCredentials
        implements AWSCredentials
{
    private String accessKeyId;
    private String secretKey;

    @Inject
    public KinesisAwsCredentials(String accessKeyId, String secretKey)
    {
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
    }

    @Override
    public String getAWSAccessKeyId()
    {
        return accessKeyId;
    }

    @Override
    public String getAWSSecretKey()
    {
        return secretKey;
    }
}
