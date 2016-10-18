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

public class InternalCommunicationConfig
{
    private boolean httpsRequired;
    private String keyStorePath;
    private String keyStorePassword;
    private String ldapUser;
    private String ldapPassword;

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
}
