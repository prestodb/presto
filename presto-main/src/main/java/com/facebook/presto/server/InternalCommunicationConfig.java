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
package com.facebook.presto.server;

import io.airlift.configuration.Config;

import java.io.File;

public class InternalCommunicationConfig
{
    private boolean httpsRequired;
    private String keyStorePath;
    private String keyStorePassword;
    private String ldapUser;
    private String ldapPassword;
    private String kerberosPrincipal;
    private String kerberosServiceName;
    private boolean kerberosEnabled;
    private File kerberosKeytab;
    private File kerberosConfig;
    private boolean kerberosUseCanonicalHostname = true;
    private File kerberosCredentialCache;

    public boolean isHttpsRequired()
    {
        return httpsRequired;
    }

    @Config("internal-communication.https.required")
    public InternalCommunicationConfig setHttpsRequired(boolean httpsRequired)
    {
        this.httpsRequired = httpsRequired;
        return this;
    }

    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    @Config("internal-communication.https.keystore.path")
    public InternalCommunicationConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("internal-communication.https.keystore.key")
    public InternalCommunicationConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public String getLdapUser()
    {
        return ldapUser;
    }

    @Config("internal-communication.authentication.ldap.user")
    public void setLdapUser(String ldapUser)
    {
        this.ldapUser = ldapUser;
    }

    public String getLdapPassword()
    {
        return ldapPassword;
    }

    @Config("internal-communication.authentication.ldap.password")
    public void setLdapPassword(String ldapPassword)
    {
        this.ldapPassword = ldapPassword;
    }

    public boolean isKerberosEnabled()
    {
        return kerberosEnabled;
    }

    @Config("internal-communication.authentication.kerberos.enabled")
    public InternalCommunicationConfig setKerberosEnabled(boolean kerberosEnabled)
    {
        this.kerberosEnabled = kerberosEnabled;
        return this;
    }

    public String getKerberosPrincipal()
    {
        return kerberosPrincipal;
    }

    @Config("internal-communication.authentication.krb5.principal")
    public InternalCommunicationConfig setKerberosPrincipal(String kerberosPrincipal)
    {
        this.kerberosPrincipal = kerberosPrincipal;
        return this;
    }

    public String getKerberosServiceName()
    {
        return kerberosServiceName;
    }

    @Config("internal-communication.authentication.krb5.service-name")
    public InternalCommunicationConfig setKerberosServiceName(String kerberosServiceName)
    {
        this.kerberosServiceName = kerberosServiceName;
        return this;
    }

    public File getKerberosKeytab()
    {
        return kerberosKeytab;
    }

    @Config("internal-communication.authentication.krb5.keytab")
    public InternalCommunicationConfig setKerberosKeytab(File kerberosKeytab)
    {
        this.kerberosKeytab = kerberosKeytab;
        return this;
    }

    public File getKerberosConfig()
    {
        return kerberosConfig;
    }

    @Config("internal-communication.authentication.krb5.config")
    public InternalCommunicationConfig setKerberosConfig(File kerberosConfig)
    {
        this.kerberosConfig = kerberosConfig;
        return this;
    }

    public boolean isKerberosUseCanonicalHostname()
    {
        return kerberosUseCanonicalHostname;
    }

    @Config("internal-communication.authentication.krb5.use-canonical-hostname")
    public InternalCommunicationConfig setKerberosUseCanonicalHostname(boolean kerberosUseCanonicalHostname)
    {
        this.kerberosUseCanonicalHostname = kerberosUseCanonicalHostname;
        return this;
    }

    public File getKerberosCredentialCache()
    {
        return kerberosCredentialCache;
    }

    @Config("internal-communication.authentication.krb5.credential-cache")
    public InternalCommunicationConfig setKerberosCredentialCache(File kerberosCredentialCache)
    {
        this.kerberosCredentialCache = kerberosCredentialCache;
        return this;
    }
}
