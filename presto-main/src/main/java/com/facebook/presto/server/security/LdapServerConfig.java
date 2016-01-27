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

public class LdapServerConfig
{
    private String ldapURL = "ldaps://localhost:636/";
    private String ldapSystemUser;
    private String ldapSystemPassword;
    private String ldapSearchBase;
    private boolean authenticationEnabled = false;

    public String getSystemUser()
    {
        return ldapSystemUser;
    }

    public String getSystemPassword()
    {
        return ldapSystemPassword;
    }

    public String getURL()
    {
        return ldapURL;
    }

    public String getSearchBase()
    {
        return ldapSearchBase;
    }

    public boolean getAuthenticationEnabled()
    {
        return authenticationEnabled;
    }

    @Config("ldap.url")
    public LdapServerConfig setURL(String url)
    {
        this.ldapURL = url;
        return this;
    }

    @Config("ldap.systemUsername")
    public LdapServerConfig setSystemUser(String user)
    {
        this.ldapSystemUser = user;
        return this;
    }

    @Config("ldap.systemPassword")
    public LdapServerConfig setSystemPassword(String password)
    {
        this.ldapSystemPassword = password;
        return this;
    }

    @Config("ldap.searchBase")
    public LdapServerConfig setSearchBase(String searchBase)
    {
        this.ldapSearchBase = searchBase;
        return this;
    }

    @Config("ldap.authentication.enabled")
    public LdapServerConfig setAuthenticationEnabled(boolean enabled)
    {
        this.authenticationEnabled = enabled;
        return this;
    }
}
