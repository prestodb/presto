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

import com.google.common.base.Throwables;
import com.google.common.net.HttpHeaders;
import com.sun.security.auth.module.Krb5LoginModule;
import io.airlift.log.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.ietf.jgss.GSSCredential.ACCEPT_ONLY;
import static org.ietf.jgss.GSSCredential.INDEFINITE_LIFETIME;

public class SpnegoFilter
        implements Filter
{
    private static final Logger LOG = Logger.get(SpnegoFilter.class);

    private static final String NEGOTIATE_SCHEME = "Negotiate";
    private static final String INCLUDE_REALM_HEADER = "X-Airlift-Realm-In-Challenge";

    private final GSSManager gssManager = GSSManager.getInstance();
    private final LoginContext loginContext;
    private final GSSCredential serverCredential;

    @Inject
    public SpnegoFilter(SecurityConfig config)
    {
        System.setProperty("java.security.krb5.conf", config.getKerberosConfig().getAbsolutePath());

        try {
            String hostname = InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
            String servicePrincipal = config.getServiceName() + "/" + hostname;
            loginContext = new LoginContext("", null, null, new Configuration()
            {
                @Override
                public AppConfigurationEntry[] getAppConfigurationEntry(String name)
                {
                    Map<String, String> options = new HashMap<>();
                    options.put("refreshKrb5Config", "true");
                    options.put("doNotPrompt", "true");
                    if (LOG.isDebugEnabled()) {
                        options.put("debug", "true");
                    }
                    if (config.getKeytab() != null) {
                        options.put("keyTab", config.getKeytab().getAbsolutePath());
                    }
                    options.put("isInitiator", "false");
                    options.put("useKeyTab", "true");
                    options.put("principal", servicePrincipal);
                    options.put("storeKey", "true");

                    return new AppConfigurationEntry[] {new AppConfigurationEntry(Krb5LoginModule.class.getName(), REQUIRED, options)};
                }
            });
            loginContext.login();

            serverCredential = doAs(loginContext.getSubject(), () -> gssManager.createCredential(
                    gssManager.createName(config.getServiceName() + "@" + hostname, GSSName.NT_HOSTBASED_SERVICE),
                    INDEFINITE_LIFETIME,
                    new Oid[] {
                            new Oid("1.2.840.113554.1.2.2"), // kerberos 5
                            new Oid("1.3.6.1.5.5.2") // spnego
                    },
                    ACCEPT_ONLY));
        }
        catch (LoginException | UnknownHostException e) {
            throw Throwables.propagate(e);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            loginContext.logout();
        }
        catch (LoginException e) {
            Throwables.propagate(e);
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

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);

        boolean includeRealm = "true".equalsIgnoreCase(request.getHeader(INCLUDE_REALM_HEADER));

        if (header != null) {
            String[] parts = header.split("\\s+");
            if (parts.length == 2 && parts[0].equals(NEGOTIATE_SCHEME)) {
                try {
                    Optional<Result> authentication = authenticate(parts[1]);
                    if (authentication.isPresent()) {
                        authentication.get()
                                .getToken()
                                .ifPresent(token -> response.setHeader(HttpHeaders.WWW_AUTHENTICATE, formatAuthenticationHeader(includeRealm, Optional.ofNullable(token))));

                        nextFilter.doFilter(new HttpServletRequestWrapper(request)
                        {
                            @Override
                            public Principal getUserPrincipal()
                            {
                                return authentication.get().getPrincipal();
                            }
                        }, servletResponse);
                        return;
                    }
                }
                catch (GSSException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        sendChallenge(response, includeRealm);
    }

    private Optional<Result> authenticate(String token)
            throws GSSException
    {
        GSSContext context = doAs(loginContext.getSubject(), () -> gssManager.createContext(serverCredential));

        try {
            byte[] inputToken = Base64.getDecoder().decode(token);
            byte[] outputToken = context.acceptSecContext(inputToken, 0, inputToken.length);

            // We can't hold on to the GSS context because HTTP is stateless, so fail
            // if it can't be set up in a single challenge-response cycle
            if (context.isEstablished()) {
                return Optional.of(new Result(
                        Optional.ofNullable(outputToken),
                        new KerberosPrincipal(context.getSrcName().toString())));
            }
            LOG.debug("Failed to establish GSS context for token %s", token);
        }
        catch (GSSException e) {
            // ignore and fail the authentication
            LOG.debug(e, "Authentication failed for token %s", token);
        }
        finally {
            try {
                context.dispose();
            }
            catch (GSSException e) {
                // ignore
            }
        }

        return Optional.empty();
    }

    private static void sendChallenge(HttpServletResponse response, boolean includeRealm)
    {
        response.setStatus(SC_UNAUTHORIZED);
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, formatAuthenticationHeader(includeRealm, Optional.empty()));
    }

    private static String formatAuthenticationHeader(boolean includeRealm, Optional<byte[]> token)
    {
        StringBuilder header = new StringBuilder(NEGOTIATE_SCHEME);

        if (includeRealm) {
            header.append(" realm=\"presto\"");
        }

        if (token.isPresent()) {
            header.append(" ")
                    .append(Base64.getEncoder().encodeToString(token.get()));
        }

        return header.toString();
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

    private interface GssSupplier<T>
    {
        T get()
                throws GSSException;
    }

    private static <T> T doAs(Subject subject, GssSupplier<T> action)
    {
        return Subject.doAs(subject, (PrivilegedAction<T>) () -> {
            try {
                return action.get();
            }
            catch (GSSException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    private static class Result
    {
        private final Optional<byte[]> token;
        private final KerberosPrincipal principal;

        public Result(Optional<byte[]> token, KerberosPrincipal principal)
        {
            this.token = token;
            this.principal = principal;
        }

        public Optional<byte[]> getToken()
        {
            return token;
        }

        public KerberosPrincipal getPrincipal()
        {
            return principal;
        }
    }
}
