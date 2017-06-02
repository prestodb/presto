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
package com.facebook.presto.raptor.security;

import com.facebook.presto.plugin.base.security.AllowAllAccessControl;
import com.facebook.presto.raptor.RaptorAccessControl;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder ignored)
    {
        install(installModuleIf(
                SecurityConfig.class,
                config -> config.isEnabled(),
                binder -> {
                    binder.bind(ConnectorAccessControl.class).to(RaptorAccessControl.class).in(Scopes.SINGLETON);
                }));

        install(installModuleIf(
                SecurityConfig.class,
                config -> config.isEnabled()
                        && config.getIdentityManager().equalsIgnoreCase("file"),
                binder -> {
                    install(new FileBasedIdentityModule());
                }));

        install(installModuleIf(
                SecurityConfig.class,
                config -> !config.isEnabled(),
                binder -> {
                    binder.bind(ConnectorAccessControl.class).to(AllowAllAccessControl.class).in(Scopes.SINGLETON);
                }));
    }

    private static class FileBasedIdentityModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(FileBasedIdentityConfig.class);
            binder.bind(IdentityManager.class).to(FileBasedIdentityManager.class).in(Scopes.SINGLETON);
        }
    }
}
