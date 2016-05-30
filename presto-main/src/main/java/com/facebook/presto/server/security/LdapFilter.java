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
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.Principal;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.server.security.util.jndi.JndiUtils.getInitialDirContext;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.airlift.http.client.HttpStatus.BAD_REQUEST;
import static io.airlift.http.client.HttpStatus.INTERNAL_SERVER_ERROR;
import static io.airlift.http.client.HttpStatus.UNAUTHORIZED;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static javax.naming.Context.PROVIDER_URL;
import static javax.naming.Context.SECURITY_AUTHENTICATION;
import static javax.naming.Context.SECURITY_CREDENTIALS;
import static javax.naming.Context.SECURITY_PRINCIPAL;

public class LdapFilter
        implements Filter
{
    private static final Logger log = Logger.get(LdapFilter.class);

    private static final String BASIC_AUTHENTICATION_PREFIX = "Basic ";
    private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    private final String ldapUrl;
    private final String userBindSearchPattern;
    private final Optional<String> groupAuthorizationSearchPattern;
    private final Optional<String> userBaseDistinguishedName;
    private final Map<String, String> basicEnvironment;

    @Inject
    public LdapFilter(LdapConfig serverConfig)
    {
        this.ldapUrl = requireNonNull(serverConfig.getLdapUrl(), "ldapUrl is null");
        this.userBindSearchPattern = requireNonNull(serverConfig.getUserBindSearchPattern(), "userBindSearchPattern is null");
        this.groupAuthorizationSearchPattern = Optional.ofNullable(serverConfig.getGroupAuthorizationSearchPattern());
        this.userBaseDistinguishedName = Optional.ofNullable(serverConfig.getUserBaseDistinguishedName());
        ImmutableMap<String, String> environment = ImmutableMap.of(
                INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY,
                PROVIDER_URL, ldapUrl);
        checkEnvironment(environment);
        this.basicEnvironment = environment;
    }

    private static void checkEnvironment(Map<String, String> environment)
    {
        try {
            InitialDirContext authenticate = createDirContext(environment);
            closeContext(authenticate);
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
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        // skip auth for http
        if (!servletRequest.isSecure()) {
            nextFilter.doFilter(servletRequest, servletResponse);
            return;
        }

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        try {
            String header = request.getHeader(AUTHORIZATION);
            List<String> credentials = getCredentials(header);
            Principal principal = authenticate(credentials);

            // ldap authentication ok, continue
            nextFilter.doFilter(new HttpServletRequestWrapper(request)
            {
                @Override
                public Principal getUserPrincipal()
                {
                    return principal;
                }
            }, servletResponse);
        }
        catch (AuthenticationException e) {
            log.debug(e, "LDAP authentication failed");
            processAuthenticationException(e, response);
        }
    }

    private static void processAuthenticationException(AuthenticationException e, HttpServletResponse response)
            throws IOException
    {
        if (e.getStatus() == UNAUTHORIZED) {
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"presto\"");
        }
        response.sendError(e.getStatus().code(), e.getMessage());
    }

    private static List<String> getCredentials(String header)
            throws AuthenticationException
    {
        if (header == null) {
            throw new AuthenticationException(UNAUTHORIZED, "Unauthorized");
        }
        if (!header.startsWith(BASIC_AUTHENTICATION_PREFIX)) {
            throw new AuthenticationException(BAD_REQUEST, "Basic authentication is expected");
        }
        String base64EncodedCredentials = header.substring(BASIC_AUTHENTICATION_PREFIX.length());
        String credentials = decodeCredentials(base64EncodedCredentials);
        List<String> parts = Splitter.on(':').limit(2).splitToList(credentials);
        if (parts.size() != 2 || parts.stream().anyMatch(String::isEmpty)) {
            throw new AuthenticationException(BAD_REQUEST, "Invalid credentials: " + credentials);
        }
        return parts;
    }

    private static String decodeCredentials(String base64EncodedCredentials)
            throws AuthenticationException
    {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(base64EncodedCredentials);
        }
        catch (IllegalArgumentException e) {
            throw new AuthenticationException(BAD_REQUEST, "Invalid base64 string: " + base64EncodedCredentials);
        }
        return new String(bytes, UTF_8);
    }

    private Principal authenticate(List<String> credentials)
            throws AuthenticationException
    {
        Map<String, String> environment = createEnvironment(credentials);
        String user = credentials.get(0);
        InitialDirContext context = null;
        try {
            context = createDirContext(environment);
            checkForGroupMembership(user, context);

            log.debug("Authentication successful for user %s.", user);
            return new LdapPrincipal(user);
        }
        catch (javax.naming.AuthenticationException e) {
            String formattedAsciiMessage = format("Invalid credentials: %s", e.getMessage().replaceAll("[^\\x20-\\x7e]", ""));
            log.debug("Authentication failed for user %s. %s", user, formattedAsciiMessage);
            throw new AuthenticationException(UNAUTHORIZED, formattedAsciiMessage, e);
        }
        catch (NamingException e) {
            throw new AuthenticationException(INTERNAL_SERVER_ERROR, "Authentication failed", e);
        }
        finally {
            closeContext(context);
        }
    }

    private Map<String, String> createEnvironment(List<String> credentials)
    {
        ImmutableMap.Builder<String, String> environment = ImmutableMap.builder();
        environment.putAll(basicEnvironment);
        environment.put(SECURITY_AUTHENTICATION, "simple");
        environment.put(SECURITY_PRINCIPAL, createPrincipal(credentials.get(0)));
        environment.put(SECURITY_CREDENTIALS, credentials.get(1));
        return environment.build();
    }

    private String createPrincipal(String user)
    {
        return userBindSearchPattern.replaceAll("\\$\\{USER\\}", user);
    }

    private void checkForGroupMembership(String user, DirContext context)
            throws AuthenticationException
    {
        if (!groupAuthorizationSearchPattern.isPresent()) {
            return;
        }

        checkState(userBaseDistinguishedName.isPresent(), "Base distinguished name (DN) for user %s is null", user);

        String searchFilter = groupAuthorizationSearchPattern.get().replaceAll("\\$\\{USER\\}", user);
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        boolean authorized;
        NamingEnumeration<SearchResult> search = null;
        try {
            search = context.search(userBaseDistinguishedName.get(), searchFilter, searchControls);
            authorized = search.hasMoreElements();
        }
        catch (NamingException e) {
            throw new AuthenticationException(INTERNAL_SERVER_ERROR, "Authentication failed", e);
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
            String message = format("Unauthorized user: User %s not a member of the authorized group", user);
            log.debug("Authorization failed for user. " + message);
            throw new AuthenticationException(UNAUTHORIZED, message);
        }
        log.debug("Authorization succeeded for user %s", user);
    }

    @Override
    public void init(FilterConfig filterConfig)
            throws ServletException
    {
    }

    @Override
    public void destroy()
    {
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

    private static class AuthenticationException
            extends Exception
    {
        private final HttpStatus status;

        private AuthenticationException(HttpStatus status, String message)
        {
            this(status, message, null);
        }

        private AuthenticationException(HttpStatus status, String message, Throwable cause)
        {
            super(message, cause);
            requireNonNull(message, "message is null");
            this.status = requireNonNull(status, "status is null");
        }

        public HttpStatus getStatus()
        {
            return status;
        }
    }
}
