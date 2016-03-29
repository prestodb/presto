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
package org.apache.hadoop.security;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.util.Set;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;

public final class UserGroupInformationShim
{
    private UserGroupInformationShim() {}

    public static Subject getSubject(UserGroupInformation userGroupInformation)
    {
        return userGroupInformation.getSubject();
    }

    public static UserGroupInformation createUserGroupInformationForSubject(Subject subject)
    {
        if (subject == null) {
            throw new NullPointerException("subject is null");
        }
        Set<KerberosPrincipal> kerberosPrincipals = subject.getPrincipals(KerberosPrincipal.class);
        if (kerberosPrincipals.isEmpty()) {
            throw new IllegalArgumentException("subject must contain a KerberosPrincipal");
        }
        if (kerberosPrincipals.size() != 1) {
            throw new IllegalArgumentException("subject must contain only a single KerberosPrincipal");
        }

        KerberosPrincipal principal = kerberosPrincipals.iterator().next();
        User user = new User(principal.getName(), KERBEROS, null);
        subject.getPrincipals().add(user);
        UserGroupInformation userGroupInformation = new UserGroupInformation(subject);
        userGroupInformation.setAuthenticationMethod(KERBEROS);
        return userGroupInformation;
    }
}
