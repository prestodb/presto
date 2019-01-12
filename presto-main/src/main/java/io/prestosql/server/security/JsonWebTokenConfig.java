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
package io.prestosql.server.security;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class JsonWebTokenConfig
{
    private String keyFile;
    private String requiredIssuer;
    private String requiredAudience;

    @NotNull
    public String getKeyFile()
    {
        return keyFile;
    }

    @Config("http.authentication.jwt.key-file")
    public JsonWebTokenConfig setKeyFile(String keyFile)
    {
        this.keyFile = keyFile;
        return this;
    }

    public String getRequiredIssuer()
    {
        return requiredIssuer;
    }

    @Config("http.authentication.jwt.required-issuer")
    public JsonWebTokenConfig setRequiredIssuer(String requiredIssuer)
    {
        this.requiredIssuer = requiredIssuer;
        return this;
    }

    public String getRequiredAudience()
    {
        return requiredAudience;
    }

    @Config("http.authentication.jwt.required-audience")
    public JsonWebTokenConfig setRequiredAudience(String requiredAudience)
    {
        this.requiredAudience = requiredAudience;
        return this;
    }
}
