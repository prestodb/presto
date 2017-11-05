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

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.servlet.http.HttpServletRequest;

import java.security.Principal;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.server.security.util.jndi.JndiUtils.getInitialDirContext;
import static com.google.common.base.CharMatcher.javaIsoControl;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static javax.naming.Context.PROVIDER_URL;
import static javax.naming.Context.SECURITY_AUTHENTICATION;
import static javax.naming.Context.SECURITY_CREDENTIALS;
import static javax.naming.Context.SECURITY_PRINCIPAL;

public class LdapAuthenticator
        implements Authenticator
{
    private static final Logger log = Logger.get(LdapAuthenticator.class);

    private static final String BASIC_AUTHENTICATION_PREFIX = "Basic ";
    private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    private final String ldapUrl;
    private final String userBindSearchPattern;
    private final Optional<String> groupAuthorizationSearchPattern;
    private final Optional<String> userBaseDistinguishedName;
    private final Map<String, String> basicEnvironment;
    private final LoadingCache<Credentials, Principal> authenticationCache;

    @Inject
    public LdapAuthenticator(LdapConfig serverConfig)
    {
        this.ldapUrl = requireNonNull(serverConfig.getLdapUrl(), "ldapUrl is null");
        this.userBindSearchPattern = requireNonNull(serverConfig.getUserBindSearchPattern(), "userBindSearchPattern is null");
        this.groupAuthorizationSearchPattern = Optional.ofNullable(serverConfig.getGroupAuthorizationSearchPattern());
        this.userBaseDistinguishedName = Optional.ofNullable(serverConfig.getUserBaseDistinguishedName());
        if (groupAuthorizationSearchPattern.isPresent()) {
            checkState(userBaseDistinguishedName.isPresent(), "Base distinguished name (DN) for user is null");
        }

        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put(INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY)
                .put(PROVIDER_URL, ldapUrl)
                .build();
        checkEnvironment(environment);
        this.basicEnvironment = environment;
        this.authenticationCache = CacheBuilder.newBuilder()
                .expireAfterWrite(serverConfig.getLdapCacheTtl().toMillis(), MILLISECONDS)
                .build(new CacheLoader<Credentials, Principal>()
                {
                    @Override
                    public Principal load(@Nonnull Credentials key)
                            throws AuthenticationException
                    {
                        return authenticate(key.getUser(), key.getPassword());
                    }
                });
    }

    private static void checkEnvironment(Map<String, String> environment)
    {
        try {
            closeContext(createDirContext(environment));
        }
        catch (NamingException e) {
            throw Throwables.propagate(e);
        }
    }

    private static InitialDirContext createDirContext(Map<String, String> environment)
            throws NamingException
    {
        return getInitialDirContext(environment);
    }

    private static void closeContext(InitialDirContext context)
    {
        if (context != null) {
            try {
                context.close();
            }
            catch (NamingException ignore) {
            }
        }
    }

    @Override
    public Principal authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        String header = request.getHeader(AUTHORIZATION);

        if (nullToEmpty(header).startsWith(BASIC_AUTHENTICATION_PREFIX)) {
            String value = header.substring(BASIC_AUTHENTICATION_PREFIX.length()).trim();
            return getPrincipal(getCredentials(value));
        }

        throw needAuthentication(null);
    }

    private Principal getPrincipal(Credentials credentials)
            throws AuthenticationException
    {
        try {
            return authenticationCache.get(credentials);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                throwIfInstanceOf(cause, AuthenticationException.class);
            }
            throw new RuntimeException(cause);
        }
    }

    private static AuthenticationException needAuthentication(String message)
            throws AuthenticationException
    {
        return new AuthenticationException(message, "Basic realm=\"presto\"");
    }

    private static Credentials getCredentials(String base64EncodedCredentials)
            throws AuthenticationException
    {
        String credentials = decodeCredentials(base64EncodedCredentials);
        List<String> parts = Splitter.on(':').limit(2).splitToList(credentials);
        if (parts.size() != 2 || parts.stream().anyMatch(String::isEmpty)) {
            throw new AuthenticationException("Malformed decoded credentials");
        }
        return new Credentials(parts.get(0), parts.get(1));
    }

    private static String decodeCredentials(String base64EncodedCredentials)
            throws AuthenticationException
    {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(base64EncodedCredentials);
        }
        catch (IllegalArgumentException e) {
            throw new AuthenticationException("Invalid base64 encoded credentials");
        }
        return new String(bytes, UTF_8);
    }

    private Principal authenticate(String user, String password)
            throws AuthenticationException
    {
        Map<String, String> environment = createEnvironment(user, password);
        InitialDirContext context = null;
        try {
            context = createDirContext(environment);
            checkForGroupMembership(user, context);

            log.debug("Authentication successful for user [%s]", user);
            return new LdapPrincipal(user);
        }
        catch (javax.naming.AuthenticationException e) {
            log.debug("Authentication failed for user [%s]: %s", user, e.getMessage());
            throw needAuthentication("Invalid credentials: " + javaIsoControl().removeFrom(e.getMessage()));
        }
        catch (NamingException e) {
            log.debug("Authentication failed for user [%s]: %s", user, e.getMessage());
            throw new AuthenticationException("Authentication failed");
        }
        finally {
            closeContext(context);
        }
    }

    private Map<String, String> createEnvironment(String user, String password)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(basicEnvironment)
                .put(SECURITY_AUTHENTICATION, "simple")
                .put(SECURITY_PRINCIPAL, createPrincipal(user))
                .put(SECURITY_CREDENTIALS, password)
                .build();
    }

    private String createPrincipal(String user)
    {
        return replaceUser(userBindSearchPattern, user);
    }

    private String replaceUser(String pattern, String user)
    {
        return pattern.replaceAll("\\$\\{USER\\}", user);
    }

    private void checkForGroupMembership(String user, DirContext context)
            throws AuthenticationException
    {
        if (!groupAuthorizationSearchPattern.isPresent()) {
            return;
        }

        String searchFilter = replaceUser(groupAuthorizationSearchPattern.get(), user);
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        boolean authorized;
        NamingEnumeration<SearchResult> search = null;
        try {
            search = context.search(userBaseDistinguishedName.get(), searchFilter, searchControls);
            authorized = search.hasMoreElements();
        }
        catch (NamingException e) {
            log.debug("Authentication failed for user [%s]: %s", user, e.getMessage());
            throw new AuthenticationException("Authentication failed");
        }
        finally {
            if (search != null) {
                try {
                    search.close();
                }
                catch (NamingException ignore) {
                }
            }
        }

        if (!authorized) {
            String message = format("User [%s] not a member of the authorized group", user);
            log.debug("Group check failed. %s", message);
            throw needAuthentication(message);
        }
    }

    private static final class LdapPrincipal
            implements Principal
    {
        private final String name;

        private LdapPrincipal(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LdapPrincipal that = (LdapPrincipal) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    private static class Credentials
    {
        private final String user;
        private final String password;

        private Credentials(String user, String password)
        {
            this.user = requireNonNull(user);
            this.password = requireNonNull(password);
        }

        public String getUser()
        {
            return user;
        }

        public String getPassword()
        {
            return password;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Credentials that = (Credentials) o;

            return Objects.equals(this.user, that.user) &&
                    Objects.equals(this.password, that.password);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, password);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("user", user)
                    .add("password", password)
                    .toString();
        }
    }
}
