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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.TheServlet;

import javax.servlet.Filter;

import java.util.function.Predicate;

import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.KERBEROS;
import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.LDAP;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class ServerSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder securityBinder)
    {
        bindSecurityConfig(
                securityConfig -> securityConfig.getAuthenticationType() == KERBEROS,
                binder -> {
                    configBinder(binder).bindConfig(KerberosConfig.class);
                    Multibinder.newSetBinder(binder, Filter.class, TheServlet.class)
                            .addBinding()
                            .to(SpnegoFilter.class)
                            .in(Scopes.SINGLETON);
                });

        bindSecurityConfig(
                securityConfig -> securityConfig.getAuthenticationType() == LDAP,
                binder -> {
                    configBinder(binder).bindConfig(LdapConfig.class);
                    Multibinder.newSetBinder(binder, Filter.class, TheServlet.class)
                            .addBinding()
                            .to(LdapFilter.class)
                            .in(Scopes.SINGLETON);
                });
    }

    private void bindSecurityConfig(Predicate<SecurityConfig> predicate, Module module)
    {
        install(installModuleIf(SecurityConfig.class, predicate, module));
    }
}
