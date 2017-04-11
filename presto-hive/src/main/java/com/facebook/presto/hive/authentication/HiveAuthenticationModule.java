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
package com.facebook.presto.hive.authentication;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveClientConfig.HdfsAuthenticationType;
import com.facebook.presto.hive.HiveClientConfig.HiveMetastoreAuthenticationType;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.function.Predicate;

import static com.facebook.presto.hive.authentication.AuthenticationModules.kerberosHdfsAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.kerberosHiveMetastoreAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.kerberosImpersonatingHdfsAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.noHdfsAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.noHiveMetastoreAuthenticationModule;
import static com.facebook.presto.hive.authentication.AuthenticationModules.simpleImpersonatingHdfsAuthenticationModule;
import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class HiveAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindAuthenticationModule(
                config -> config.getHiveMetastoreAuthenticationType() == HiveMetastoreAuthenticationType.NONE,
                noHiveMetastoreAuthenticationModule());

        bindAuthenticationModule(
                config -> config.getHiveMetastoreAuthenticationType() == HiveMetastoreAuthenticationType.KERBEROS,
                kerberosHiveMetastoreAuthenticationModule());

        bindAuthenticationModule(
                config -> noHdfsAuth(config) && !config.isHdfsImpersonationEnabled(),
                noHdfsAuthenticationModule());

        bindAuthenticationModule(
                config -> noHdfsAuth(config) && config.isHdfsImpersonationEnabled(),
                simpleImpersonatingHdfsAuthenticationModule());

        bindAuthenticationModule(
                config -> kerberosHdfsAuth(config) && !config.isHdfsImpersonationEnabled(),
                kerberosHdfsAuthenticationModule());

        bindAuthenticationModule(
                config -> kerberosHdfsAuth(config) && config.isHdfsImpersonationEnabled(),
                kerberosImpersonatingHdfsAuthenticationModule());
    }

    private void bindAuthenticationModule(Predicate<HiveClientConfig> predicate, Module module)
    {
        install(installModuleIf(HiveClientConfig.class, predicate, module));
    }

    private static boolean noHdfsAuth(HiveClientConfig config)
    {
        return config.getHdfsAuthenticationType() == HdfsAuthenticationType.NONE;
    }

    private static boolean kerberosHdfsAuth(HiveClientConfig config)
    {
        return config.getHdfsAuthenticationType() == HdfsAuthenticationType.KERBEROS;
    }
}
