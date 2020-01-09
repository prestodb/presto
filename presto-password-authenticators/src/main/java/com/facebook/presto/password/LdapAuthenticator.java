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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.BasicPrincipal;
import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.google.common.base.VerifyException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.inject.Inject;
import javax.naming.AuthenticationException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import java.security.Principal;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.password.jndi.JndiUtils.createDirContext;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static javax.naming.Context.PROVIDER_URL;
import static javax.naming.Context.REFERRAL;
import static javax.naming.Context.SECURITY_AUTHENTICATION;
import static javax.naming.Context.SECURITY_CREDENTIALS;
import static javax.naming.Context.SECURITY_PRINCIPAL;

public class LdapAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(LdapAuthenticator.class);

    private final Optional<String> bindUserDN;
    private final Optional<String> bindPassword;
    private final Optional<String> userLoginAttribute;
    private final Optional<String> userAttributeSearchFilter;
    private final String userBindSearchPattern;
    private final Optional<String> groupAuthorizationSearchPattern;
    private final Optional<String> userBaseDistinguishedName;
    private final Map<String, String> basicEnvironment;
    private final LoadingCache<Credentials, Principal> authenticationCache;

    @Inject
    public LdapAuthenticator(LdapConfig serverConfig)
    {
        String ldapUrl = requireNonNull(serverConfig.getLdapUrl(), "ldapUrl is null");
        this.bindUserDN = Optional.ofNullable(serverConfig.getBindUserDN());
        this.bindPassword = Optional.ofNullable(serverConfig.getBindPassword());
        this.userLoginAttribute = Optional.ofNullable(serverConfig.getUserLoginAttribute());
        this.userAttributeSearchFilter = Optional.ofNullable(serverConfig.getUserAttributeSearchFilter());
        this.userBindSearchPattern = requireNonNull(serverConfig.getUserBindSearchPattern(), "userBindSearchPattern is null");
        this.groupAuthorizationSearchPattern = Optional.ofNullable(serverConfig.getGroupAuthorizationSearchPattern());
        this.userBaseDistinguishedName = Optional.ofNullable(serverConfig.getUserBaseDistinguishedName());
        if (groupAuthorizationSearchPattern.isPresent()) {
            checkState(userBaseDistinguishedName.isPresent(), "Base distinguished name (DN) for user is null");
        }
        if (bindUserDN.isPresent()) {
            checkState(bindPassword.isPresent(), "Ldap bind-user password is null");
            checkState(userBaseDistinguishedName.isPresent(), "Base distinguished name (DN) for user is null");
            checkState(userLoginAttribute.isPresent(), "User login attribute to execute authentication is null");
            checkState(userAttributeSearchFilter.isPresent(), "User's attribute for comparison is null");
        }

        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put(INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(PROVIDER_URL, ldapUrl)
                .build();
        checkEnvironment(environment);
        this.basicEnvironment = environment;
        this.authenticationCache = CacheBuilder.newBuilder()
                .expireAfterWrite(serverConfig.getLdapCacheTtl().toMillis(), MILLISECONDS)
                .build(CacheLoader.from(this::authenticate));
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        try {
            return authenticationCache.getUnchecked(new Credentials(user, password));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), AccessDeniedException.class);
            throw e;
        }
    }

    private Principal authenticate(Credentials credentials)
    {
        return authenticate(credentials.getUser(), credentials.getPassword());
    }

    private String getLdapUserName(String defaultUserName)
            throws AuthenticationException
    {
        if (!bindUserDN.isPresent()) {
            return defaultUserName;
        }
        DirContext context = null;
        String ldapBindUser = bindUserDN.orElseThrow(VerifyException::new);
        String ldapBindPassword = bindPassword.orElseThrow(VerifyException::new);
        String userBase = userBaseDistinguishedName.orElseThrow(VerifyException::new);
        String loginAttribute = userLoginAttribute.orElseThrow(VerifyException::new);
        String userSearchFilter = userAttributeSearchFilter.orElseThrow(VerifyException::new);

        Map<String, String> environment = createEnvironment(ldapBindUser, ldapBindPassword);

        try {
            context = createDirContext(environment);
            SearchControls searchControls = getSearchControl();
            searchControls.setReturningAttributes(new String[] {loginAttribute});
            String searchFilter = userSearchFilter + "=" + defaultUserName; //TODO: Escape special characters. Make sure there is no inejction
            NamingEnumeration<SearchResult> search = context.search(userBase, searchFilter, searchControls);
            SearchResult result = getUniqueUser(search);
            Attributes attributes = result.getAttributes();
            Attribute userPrincipalName = attributes.get(loginAttribute);
            search.close();
            Object attributeValue = userPrincipalName.get();
            checkState(attributeValue instanceof String);
            return (String) attributeValue;
        }
        catch (AuthenticationException e) {
            log.debug("Authentication failed for bind user [%s]: %s", ldapBindUser, e.getMessage());
            throw new AuthenticationException("Invalid credentials or User not found");
        }
        catch (NamingException e) {
            log.debug(e, "Authentication error for user [%s]", ldapBindUser);
            throw new AuthenticationException("Authentication error");
        }
        finally {
            if (context != null) {
                closeContext(context);
            }
        }
    }

    private SearchResult getUniqueUser(NamingEnumeration<SearchResult> search)
            throws NamingException
    {
        if (!search.hasMoreElements()) {
            throw new NonUniqueResultException("User not found matching the search filter");
        }
        SearchResult result = search.next(); // TODO: Test if this works in case no, 1 and more than 1 users are returned
        if (search.hasMoreElements()) {
            throw new NonUniqueResultException("More than one User found matching the search filter");
        }
        return result;
    }

    private Principal authenticate(String user, String password)
    {
        DirContext context = null;
        try {
            String ldapUserName = getLdapUserName(user);
            Map<String, String> environment = createEnvironment(ldapUserName, password);
            context = createDirContext(environment);
            checkForGroupMembership(user, context);
            log.debug("Authentication successful for user [%s]", ldapUserName);
            return new BasicPrincipal(user);
        }
        catch (AuthenticationException e) {
            log.debug("Authentication failed for user [%s]: %s", user, e.getMessage());
            throw new AccessDeniedException("Invalid credentials");
        }
        catch (NamingException e) {
            log.debug(e, "Authentication error for user [%s]", user);
            throw new RuntimeException("Authentication error");
        }
        finally {
            if (context != null) {
                closeContext(context);
            }
        }
    }

    private static class NonUniqueResultException
            extends RuntimeException
    {
        public NonUniqueResultException(String message)
        {
            super(message);
        }
    }

    private Map<String, String> createEnvironment(String user, String password)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(basicEnvironment)
                .put(SECURITY_AUTHENTICATION, "simple")
                .put(SECURITY_PRINCIPAL, createPrincipal(user))
                .put(SECURITY_CREDENTIALS, password)
                .put(REFERRAL, "follow")
                .build();
    }

    private String createPrincipal(String user)
    {
        return replaceUser(userBindSearchPattern, user);
    }

    private void checkForGroupMembership(String user, DirContext context)
    {
        if (!groupAuthorizationSearchPattern.isPresent()) {
            return;
        }

        String userBase = userBaseDistinguishedName.orElseThrow(VerifyException::new);
        String searchFilter = replaceUser(groupAuthorizationSearchPattern.get(), user);
        SearchControls searchControls = getSearchControl();

        boolean authorized;
        try {
            NamingEnumeration<SearchResult> search = context.search(userBase, searchFilter, searchControls);
            authorized = search.hasMoreElements();
            search.close();
        }
        catch (NamingException e) {
            log.debug("Authentication error for user [%s]: %s", user, e.getMessage());
            throw new RuntimeException("Authentication error");
        }

        if (!authorized) {
            String message = format("User [%s] not a member of the authorized group", user);
            log.debug(message);
            throw new AccessDeniedException(message);
        }
    }

    private SearchControls getSearchControl()
    {
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        return searchControls;
    }

    private static String replaceUser(String pattern, String user)
    {
        return pattern.replaceAll("\\$\\{USER}", user);
    }

    private static void checkEnvironment(Map<String, String> environment)
    {
        try {
            closeContext(createDirContext(environment));
        }
        catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeContext(DirContext context)
    {
        try {
            context.close();
        }
        catch (NamingException ignored) {
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
