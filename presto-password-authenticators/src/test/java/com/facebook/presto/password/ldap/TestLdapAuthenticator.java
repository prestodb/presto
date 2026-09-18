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
package com.facebook.presto.password.ldap;

import com.facebook.presto.spi.security.AccessDeniedException;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLdapAuthenticator
{
    private static LdapAuthenticator createAuthenticator()
    {
        LdapConfig config = new LdapConfig()
                .setLdapUrl("ldaps://ldap.example.com:636")
                .setUserBindSearchPattern("uid=${USER},ou=users,dc=example,dc=com")
                .setUserBaseDistinguishedName("dc=example,dc=com")
                .setGroupAuthorizationSearchPattern("(&(objectClass=inetOrgPerson)(memberOf=cn=presto,dc=example,dc=com)(uid=${USER}))");
        return new LdapAuthenticator(config);
    }

    @Test
    public void testRejectsUsernameWithLdapSpecialCharacters()
    {
        LdapAuthenticator authenticator = createAuthenticator();
        // Metacharacters that would otherwise be interpolated into the bind DN and group search filter.
        for (String user : new String[] {"*", "admin)(uid=*", "a\\29", "user,ou=admins", "a=b", "a(b)c"}) {
            assertThatThrownBy(() -> authenticator.createAuthenticatedPrincipal(user, "password"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("special LDAP character");
        }
    }
}
