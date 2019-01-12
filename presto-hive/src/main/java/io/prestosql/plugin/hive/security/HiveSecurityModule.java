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
package io.prestosql.plugin.hive.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.base.security.FileBasedAccessControlModule;
import io.prestosql.plugin.base.security.ReadOnlySecurityModule;

import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class HiveSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindSecurityModule("legacy", new LegacySecurityModule());
        bindSecurityModule("file", new FileBasedAccessControlModule());
        bindSecurityModule("read-only", new ReadOnlySecurityModule());
        bindSecurityModule("sql-standard", new SqlStandardSecurityModule());
    }

    private void bindSecurityModule(String name, Module module)
    {
        install(installModuleIf(
                SecurityConfig.class,
                security -> name.equalsIgnoreCase(security.getSecuritySystem()),
                module));
    }
}
