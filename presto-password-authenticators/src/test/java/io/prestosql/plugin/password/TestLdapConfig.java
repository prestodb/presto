
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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestLdapConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(LdapConfig.class)
                .setLdapUrl(null)
                .setUserBindSearchPattern(null)
                .setUserBaseDistinguishedName(null)
                .setGroupAuthorizationSearchPattern(null)
                .setLdapCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("ldap.url", "ldaps://localhost:636")
                .put("ldap.user-bind-pattern", "uid=${USER},ou=org,dc=test,dc=com")
                .put("ldap.user-base-dn", "dc=test,dc=com")
                .put("ldap.group-auth-pattern", "&(objectClass=user)(memberOf=cn=group)(user=username)")
                .put("ldap.cache-ttl", "2m")
                .build();

        LdapConfig expected = new LdapConfig()
                .setLdapUrl("ldaps://localhost:636")
                .setUserBindSearchPattern("uid=${USER},ou=org,dc=test,dc=com")
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)")
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertValidates(new LdapConfig()
                .setLdapUrl("ldaps://localhost")
                .setUserBindSearchPattern("uid=${USER},ou=org,dc=test,dc=com")
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)"));

        assertFailsValidation(new LdapConfig().setLdapUrl("ldap://"), "ldapUrl", "LDAP without SSL/TLS unsupported. Expected ldaps://", Pattern.class);
        assertFailsValidation(new LdapConfig().setLdapUrl("localhost"), "ldapUrl", "LDAP without SSL/TLS unsupported. Expected ldaps://", Pattern.class);
        assertFailsValidation(new LdapConfig().setLdapUrl("ldaps:/localhost"), "ldapUrl", "LDAP without SSL/TLS unsupported. Expected ldaps://", Pattern.class);

        assertFailsValidation(new LdapConfig(), "ldapUrl", "may not be null", NotNull.class);
        assertFailsValidation(new LdapConfig(), "userBindSearchPattern", "may not be null", NotNull.class);
    }
}
