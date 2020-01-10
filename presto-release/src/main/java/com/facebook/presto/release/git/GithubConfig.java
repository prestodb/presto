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
package com.facebook.presto.release.git;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class GithubConfig
{
    private String user;
    private String accessToken;

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("github.user")
    public GithubConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public String getAccessToken()
    {
        return accessToken;
    }

    @Config("github.access-token")
    public GithubConfig setAccessToken(String accessToken)
    {
        this.accessToken = accessToken;
        return this;
    }
}
