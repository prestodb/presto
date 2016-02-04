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
package com.facebook.presto.server.security;

import io.airlift.configuration.Config;

import java.io.File;

public class SecurityConfig
{
    private boolean authenticationEnabled;
    private File kerberosConfig;
    private String serviceName;
    private File keytab;

    public File getKerberosConfig()
    {
        return kerberosConfig;
    }

    @Config("http.authentication.krb5.config")
    public SecurityConfig setKerberosConfig(File kerberosConfig)
    {
        this.kerberosConfig = kerberosConfig;
        return this;
    }

    public boolean getAuthenticationEnabled()
    {
        return authenticationEnabled;
    }

    @Config("http.server.authentication.enabled")
    public SecurityConfig setAuthenticationEnabled(boolean enabled)
    {
        this.authenticationEnabled = enabled;
        return this;
    }

    public String getServiceName()
    {
        return serviceName;
    }

    @Config("http.server.authentication.krb5.service-name")
    public SecurityConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    public File getKeytab()
    {
        return keytab;
    }

    @Config("http.server.authentication.krb5.keytab")
    public SecurityConfig setKeytab(File keytab)
    {
        this.keytab = keytab;
        return this;
    }
}
