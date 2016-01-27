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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestLdapServerConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(LdapServerConfig.class)
                .setURL("ldaps://localhost:636/")
                .setSystemUser(null)
                .setSystemPassword(null)
                .setSearchBase(null)
                .setAuthenticationEnabled(false));
    }

    @Test
    public void testExplicitConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("ldap.url", "ldaps://localhost:636")
                .put("ldap.systemUsername", "system1")
                .put("ldap.systemPassword", "password1")
                .put("ldap.searchBase", "DC=corp,DC=root,DC=mycompany,DC=com")
                .put("ldap.authentication.enabled", "true")
                .build();

        LdapServerConfig expected = new LdapServerConfig()
                .setURL("ldaps://localhost:636")
                .setSystemUser("system1")
                .setSystemPassword("password1")
                .setSearchBase("DC=corp,DC=root,DC=mycompany,DC=com")
                .setAuthenticationEnabled(true);

        assertFullMapping(properties, expected);
    }
}
