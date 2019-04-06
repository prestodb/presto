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
package com.facebook.presto.password;

import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.BasicPrincipal;
import com.facebook.presto.spi.security.PasswordAuthenticator;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.security.Principal;

/**
 * User/password authenticator that supports the various ways ODAS can be authenticated
 */
public class OkeraAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(OkeraAuthenticator.class);

    @Inject
    public OkeraAuthenticator(OkeraConfig serverConfig)
    {
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        if (!user.equals(password)) {
            log.debug("Authentication error for user [%s]", user);
            throw new AccessDeniedException("Authentication error for user: " + user);
        }
        return new BasicPrincipal(user);
    }
}
