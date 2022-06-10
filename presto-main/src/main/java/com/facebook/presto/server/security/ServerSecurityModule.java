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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.http.server.Authenticator;
import com.facebook.airlift.http.server.CertificateAuthenticator;
import com.facebook.airlift.http.server.JsonWebTokenAuthenticator;
import com.facebook.airlift.http.server.JsonWebTokenConfig;
import com.facebook.airlift.http.server.KerberosAuthenticator;
import com.facebook.airlift.http.server.KerberosConfig;
import com.facebook.presto.server.security.SecurityConfig.AuthenticationType;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import java.util.List;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.CERTIFICATE;
import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.JWT;
import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.KERBEROS;
import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.PASSWORD;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class ServerSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
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
}
