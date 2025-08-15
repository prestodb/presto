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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ProxyConfig
{
    private URI uri;
    private File sharedSecretFile;
    private Duration asyncTimeout = new Duration(2, MINUTES);

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

    @NotNull
    public Duration getAsyncTimeout()
    {
        return asyncTimeout;
    }

    @Config("proxy.async-timeout")
    @ConfigDescription("Timeout for reading responses from Coordinator to Proxy")
    public ProxyConfig setAsyncTimeout(Duration asyncTimeout)
    {
        this.asyncTimeout = asyncTimeout;
        return this;
    }
}
