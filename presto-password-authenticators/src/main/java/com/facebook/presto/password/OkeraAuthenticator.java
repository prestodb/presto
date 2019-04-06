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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.security.Principal;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * User/password authenticator that supports the various ways ODAS can be authenticated
 */
public class OkeraAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger LOG = Logger.get(OkeraAuthenticator.class);

    private final boolean authenticationEnabled;

    private final LoadingCache<LdapAuthenticator.Credentials, Principal> authenticationCache;

    private static boolean envVarSet(String name)
    {
        String v = System.getenv(name);
        return v != null && v.length() > 0;
    }

    @Inject
    public OkeraAuthenticator(OkeraConfig serverConfig)
    {
        if (envVarSet("SYSTEM_TOKEN") || envVarSet("KERBEROS_KEYTAB_FILE")) {
            // Authentication is enabled on this system if either of these are set.
            authenticationEnabled = true;
        }
        else {
            LOG.warn("Authentication is enabled on this cluster.");
            authenticationEnabled = false;
        }

        this.authenticationCache = CacheBuilder.newBuilder()
            .expireAfterWrite(serverConfig.getCacheTtl().toMillis(), MILLISECONDS)
            .build(CacheLoader.from(this::authenticate));
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        try {
            return authenticationCache.getUnchecked(new LdapAuthenticator.Credentials(user, password));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), AccessDeniedException.class);
            throw e;
        }
    }

    private Principal authenticate(LdapAuthenticator.Credentials credentials)
    {
        return authenticate(credentials.getUser(), credentials.getPassword());
    }

    private Principal authenticate(String user, String password)
    {
        if (!authenticationEnabled) {
            if (!user.equals(password)) {
                LOG.warn("Authentication error for user [%s]", user);
                throw new AccessDeniedException("Authentication error for user: " + user);
            }
            return new BasicPrincipal(user);
        }

        if (!user.equals(password)) {
            LOG.warn("Authentication error for user [%s]", user);
            throw new AccessDeniedException("Authentication error for user: " + user);
        }
        return new BasicPrincipal(user);
    }
}
