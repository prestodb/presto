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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.activedirectory.ActiveDirectoryRealm;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.subject.Subject;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNotNull;

public class TestActiveDirectoryAuthenticationModule
{
    private Injector injector;

    @BeforeSuite
    public void beforeSuite() throws Exception
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("ldap.authentication.enabled", "true");
        properties.put("ldap.url", "ldaps://localhost:636/");
        properties.put("ldap.systemUsername", "system1");
        properties.put("ldap.systemPassword", "password1");
        properties.put("ldap.searchBase", "DC=corp,DC=root,DC=company,DC=com");
        Bootstrap app = new Bootstrap(new ActiveDirectoryAuthenticationModule());
        injector = app.strictConfig().setRequiredConfigurationProperties(properties).initialize();
    }

    @Test
    public void testSecurityModules()
    {
        SecurityManager securityManager = injector.getInstance(SecurityManager.class);
        assertNotNull(securityManager);

        LdapContextFactory ldapFactory = injector.getInstance(JndiLdapContextFactory.class);
        assertNotNull(ldapFactory);

        ActiveDirectoryRealm realm = injector.getInstance(ActiveDirectoryRealm.class);
        assertNotNull(realm);

        Subject subject = injector.getInstance(Subject.class);
        assertNotNull(subject);
    }
}
