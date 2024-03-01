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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

public class HiveGcsConfig
{
    private boolean useGcsAccessToken;
    private String jsonKeyFilePath;
    private Duration accessTokenExpirationTime;

    public String getJsonKeyFilePath()
    {
        return jsonKeyFilePath;
    }

    @Config("hive.gcs.json-key-file-path")
    @ConfigDescription("JSON key file used to access Google Cloud Storage")
    public HiveGcsConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    public boolean isUseGcsAccessToken()
    {
        return useGcsAccessToken;
    }

    @Config("hive.gcs.use-access-token")
    @ConfigDescription("Use client-provided OAuth token to access Google Cloud Storage")
    public HiveGcsConfig setUseGcsAccessToken(boolean useGcsAccessToken)
    {
        this.useGcsAccessToken = useGcsAccessToken;
        return this;
    }
}
