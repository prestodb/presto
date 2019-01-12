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
package io.prestosql.server.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.TheServlet;
import io.prestosql.server.security.SecurityConfig.AuthenticationType;

import javax.servlet.Filter;

import java.util.List;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.server.security.SecurityConfig.AuthenticationType.CERTIFICATE;
import static io.prestosql.server.security.SecurityConfig.AuthenticationType.JWT;
import static io.prestosql.server.security.SecurityConfig.AuthenticationType.KERBEROS;
import static io.prestosql.server.security.SecurityConfig.AuthenticationType.PASSWORD;

public class ServerSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                .to(AuthenticationFilter.class).in(Scopes.SINGLETON);

        binder.bind(PasswordAuthenticatorManager.class).in(Scopes.SINGLETON);

        List<AuthenticationType> authTypes = buildConfigObject(SecurityConfig.class).getAuthenticationTypes();
        Multibinder<Authenticator> authBinder = newSetBinder(binder, Authenticator.class);

        for (AuthenticationType authType : authTypes) {
            if (authType == CERTIFICATE) {
                authBinder.addBinding().to(CertificateAuthenticator.class).in(Scopes.SINGLETON);
            }
            else if (authType == KERBEROS) {
                configBinder(binder).bindConfig(KerberosConfig.class);
                authBinder.addBinding().to(KerberosAuthenticator.class).in(Scopes.SINGLETON);
            }
            else if (authType == PASSWORD) {
                authBinder.addBinding().to(PasswordAuthenticator.class).in(Scopes.SINGLETON);
            }
            else if (authType == JWT) {
                configBinder(binder).bindConfig(JsonWebTokenConfig.class);
                authBinder.addBinding().to(JsonWebTokenAuthenticator.class).in(Scopes.SINGLETON);
            }
            else {
                throw new AssertionError("Unhandled auth type: " + authType);
            }
        }
    }

    @Provides
    List<Authenticator> getAuthenticatorList(Set<Authenticator> authenticators)
    {
        return ImmutableList.copyOf(authenticators);
    }
}
