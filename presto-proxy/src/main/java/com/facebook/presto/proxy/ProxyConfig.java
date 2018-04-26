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
package com.facebook.presto.proxy;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;

public class ProxyConfig
{
    private URI uri;
    private File sharedSecretFile;
    private File jwtKeyFile;
    private String jwtKeyFilePassword;
    private String jwtKeyId;
    private String jwtIssuer;
    private String jwtAudience;

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("proxy.uri")
    @ConfigDescription("URI of the remote Presto server")
    public ProxyConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    @NotNull
    public File getSharedSecretFile()
    {
        return sharedSecretFile;
    }

    @Config("proxy.shared-secret-file")
    @ConfigDescription("Shared secret file used for authenticating URIs")
    public ProxyConfig setSharedSecretFile(File sharedSecretFile)
    {
        this.sharedSecretFile = sharedSecretFile;
        return this;
    }

    public File getJwtKeyFile()
    {
        return jwtKeyFile;
    }

    @Config("proxy.jwt.key-file")
    @ConfigDescription("Key file used for generating JWT signatures")
    public ProxyConfig setJwtKeyFile(File jwtKeyFile)
    {
        this.jwtKeyFile = jwtKeyFile;
        return this;
    }

    public String getJwtKeyFilePassword()
    {
        return jwtKeyFilePassword;
    }

    @Config("proxy.jwt.key-file-password")
    @ConfigDescription("Password for encrypted key file")
    @ConfigSecuritySensitive
    public ProxyConfig setJwtKeyFilePassword(String jwtKeyFilePassword)
    {
        this.jwtKeyFilePassword = jwtKeyFilePassword;
        return this;
    }

    public String getJwtKeyId()
    {
        return jwtKeyId;
    }

    @Config("proxy.jwt.key-id")
    @ConfigDescription("Key ID for JWT")
    public ProxyConfig setJwtKeyId(String jwtKeyId)
    {
        this.jwtKeyId = jwtKeyId;
        return this;
    }

    public String getJwtIssuer()
    {
        return jwtIssuer;
    }

    @Config("proxy.jwt.issuer")
    @ConfigDescription("Issuer for JWT")
    public ProxyConfig setJwtIssuer(String jwtIssuer)
    {
        this.jwtIssuer = jwtIssuer;
        return this;
    }

    public String getJwtAudience()
    {
        return jwtAudience;
    }

    @Config("proxy.jwt.audience")
    @ConfigDescription("Audience for JWT")
    public ProxyConfig setJwtAudience(String jwtAudience)
    {
        this.jwtAudience = jwtAudience;
        return this;
    }
}
