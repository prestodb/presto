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
package io.prestosql.plugin.hive.authentication;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;

public class KerberosAuthentication
{
    private static final Logger log = Logger.get(KerberosAuthentication.class);
    private static final String KERBEROS_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    private final KerberosPrincipal principal;
    private final Configuration configuration;

    public KerberosAuthentication(String principal, String keytabLocation)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(keytabLocation, "keytabLocation is null");
        Path keytabPath = Paths.get(keytabLocation);
        checkArgument(exists(keytabPath), "keytab does not exist: " + keytabLocation);
        checkArgument(isReadable(keytabPath), "keytab is not readable: " + keytabLocation);
        this.principal = createKerberosPrincipal(principal);
        this.configuration = createConfiguration(this.principal.getName(), keytabLocation);
    }

    public Subject getSubject()
    {
        Subject subject = new Subject(false, ImmutableSet.of(principal), emptySet(), emptySet());
        try {
            LoginContext loginContext = new LoginContext("", subject, null, configuration);
            loginContext.login();
            return loginContext.getSubject();
        }
        catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    private static KerberosPrincipal createKerberosPrincipal(String principal)
    {
        try {
            return new KerberosPrincipal(getServerPrincipal(principal, InetAddress.getLocalHost().getCanonicalHostName()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Configuration createConfiguration(String principal, String keytabLocation)
    {
        ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.<String, String>builder()
                .put("useKeyTab", "true")
                .put("storeKey", "true")
                .put("doNotPrompt", "true")
                .put("isInitiator", "true")
                .put("principal", principal)
                .put("keyTab", keytabLocation);

        if (log.isDebugEnabled()) {
            optionsBuilder.put("debug", "true");
        }

        Map<String, String> options = optionsBuilder.build();

        return new Configuration()
        {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name)
            {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(
                                KERBEROS_LOGIN_MODULE,
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)};
            }
        };
    }
}
