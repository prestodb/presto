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
import javax.servlet.http.HttpServletRequest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.ietf.jgss.GSSCredential.ACCEPT_ONLY;
import static org.ietf.jgss.GSSCredential.INDEFINITE_LIFETIME;

public class KerberosAuthenticator
        implements Authenticator
{
    private static final Logger LOG = Logger.get(KerberosAuthenticator.class);

    private static final String NEGOTIATE_SCHEME = "Negotiate";

    private final GSSManager gssManager = GSSManager.getInstance();
    private final LoginContext loginContext;
    private final GSSCredential serverCredential;

    @Inject
    public KerberosAuthenticator(KerberosConfig config)
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
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            loginContext.logout();
        }
        catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Principal authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        String header = request.getHeader(AUTHORIZATION);

        String requestSpnegoToken = null;

        if (header != null) {
            String[] parts = header.split("\\s+");
            if (parts.length == 2 && parts[0].equals(NEGOTIATE_SCHEME)) {
                try {
                    requestSpnegoToken = parts[1];
                    Optional<Principal> principal = authenticate(parts[1]);
                    if (principal.isPresent()) {
                        return principal.get();
                    }
                }
                catch (GSSException | RuntimeException e) {
                    throw new RuntimeException("Authentication error for token: " + parts[1], e);
                }
            }
        }

        if (requestSpnegoToken != null) {
            throw new AuthenticationException("Authentication failed for token: " + requestSpnegoToken, NEGOTIATE_SCHEME);
        }

        throw new AuthenticationException(null, NEGOTIATE_SCHEME);
    }

    private Optional<Principal> authenticate(String token)
            throws GSSException
    {
        GSSContext context = doAs(loginContext.getSubject(), () -> gssManager.createContext(serverCredential));

        try {
            byte[] inputToken = Base64.getDecoder().decode(token);
            context.acceptSecContext(inputToken, 0, inputToken.length);

            // We can't hold on to the GSS context because HTTP is stateless, so fail
            // if it can't be set up in a single challenge-response cycle
            if (context.isEstablished()) {
                return Optional.of(new KerberosPrincipal(context.getSrcName().toString()));
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
                throw new RuntimeException(e);
            }
        });
    }
}
