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
package io.prestosql.proxy;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.io.File;

public class JwtHandlerConfig
{
    private File jwtKeyFile;
    private String jwtKeyFilePassword;
    private String jwtKeyId;
    private String jwtIssuer;
    private String jwtAudience;

    public File getJwtKeyFile()
    {
        return jwtKeyFile;
    }

    @Config("jwt.key-file")
    @ConfigDescription("Key file used for generating JWT signatures")
    public JwtHandlerConfig setJwtKeyFile(File jwtKeyFile)
    {
        this.jwtKeyFile = jwtKeyFile;
        return this;
    }

    public String getJwtKeyFilePassword()
    {
        return jwtKeyFilePassword;
    }

    @Config("jwt.key-file-password")
    @ConfigDescription("Password for encrypted key file")
    @ConfigSecuritySensitive
    public JwtHandlerConfig setJwtKeyFilePassword(String jwtKeyFilePassword)
    {
        this.jwtKeyFilePassword = jwtKeyFilePassword;
        return this;
    }

    public String getJwtKeyId()
    {
        return jwtKeyId;
    }

    @Config("jwt.key-id")
    @ConfigDescription("Key ID for JWT")
    public JwtHandlerConfig setJwtKeyId(String jwtKeyId)
    {
        this.jwtKeyId = jwtKeyId;
        return this;
    }

    public String getJwtIssuer()
    {
        return jwtIssuer;
    }

    @Config("jwt.issuer")
    @ConfigDescription("Issuer for JWT")
    public JwtHandlerConfig setJwtIssuer(String jwtIssuer)
    {
        this.jwtIssuer = jwtIssuer;
        return this;
    }

    public String getJwtAudience()
    {
        return jwtAudience;
    }

    @Config("jwt.audience")
    @ConfigDescription("Audience for JWT")
    public JwtHandlerConfig setJwtAudience(String jwtAudience)
    {
        this.jwtAudience = jwtAudience;
        return this;
    }
}
