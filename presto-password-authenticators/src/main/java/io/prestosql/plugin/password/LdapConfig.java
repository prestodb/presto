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
package io.prestosql.plugin.password;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.concurrent.TimeUnit;

public class LdapConfig
{
    private String ldapUrl;
    private String userBindSearchPattern;
    private String groupAuthorizationSearchPattern;
    private String userBaseDistinguishedName;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.HOURS);

    @NotNull
    @Pattern(regexp = "^ldaps://.*", message = "LDAP without SSL/TLS unsupported. Expected ldaps://")
    public String getLdapUrl()
    {
        return ldapUrl;
    }

    @Config("ldap.url")
    @ConfigDescription("URL of the LDAP server")
    public LdapConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    @NotNull
    public String getUserBindSearchPattern()
    {
        return userBindSearchPattern;
    }

    @Config("ldap.user-bind-pattern")
    @ConfigDescription("Custom user bind pattern. Example: ${USER}@example.com")
    public LdapConfig setUserBindSearchPattern(String userBindSearchPattern)
    {
        this.userBindSearchPattern = userBindSearchPattern;
        return this;
    }

    public String getGroupAuthorizationSearchPattern()
    {
        return groupAuthorizationSearchPattern;
    }

    @Config("ldap.group-auth-pattern")
    @ConfigDescription("Custom group authorization check query. Example: &(objectClass=user)(memberOf=cn=group)(user=username)")
    public LdapConfig setGroupAuthorizationSearchPattern(String groupAuthorizationSearchPattern)
    {
        this.groupAuthorizationSearchPattern = groupAuthorizationSearchPattern;
        return this;
    }

    public String getUserBaseDistinguishedName()
    {
        return userBaseDistinguishedName;
    }

    @Config("ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user. Example: dc=example,dc=com")
    public LdapConfig setUserBaseDistinguishedName(String userBaseDistinguishedName)
    {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    @NotNull
    public Duration getLdapCacheTtl()
    {
        return ldapCacheTtl;
    }

    @Config("ldap.cache-ttl")
    public LdapConfig setLdapCacheTtl(Duration ldapCacheTtl)
    {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }
}
