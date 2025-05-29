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
package com.facebook.presto.druid.authentication;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.presto.druid.DruidConfig;
import com.facebook.presto.druid.ForDruidClient;
import com.google.inject.Binder;
import com.google.inject.Module;

import java.util.function.Predicate;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.presto.druid.DruidConfig.DruidAuthenticationType.BASIC;
import static com.facebook.presto.druid.DruidConfig.DruidAuthenticationType.KERBEROS;
import static com.facebook.presto.druid.DruidConfig.DruidAuthenticationType.NONE;
import static java.util.Objects.requireNonNull;

public class DruidAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        DruidConfig druidConfig = buildConfigObject(DruidConfig.class);

        bindAuthenticationModule(
                config -> config.getDruidAuthenticationType() == NONE,
                noneAuthenticationModule(druidConfig));

        bindAuthenticationModule(
                config -> config.getDruidAuthenticationType() == BASIC,
                basicAuthenticationModule(druidConfig));

        bindAuthenticationModule(
                config -> config.getDruidAuthenticationType() == KERBEROS,
                kerberosbAuthenticationModule(druidConfig));
    }

    private void bindAuthenticationModule(Predicate<DruidConfig> predicate, Module module)
    {
        install(installModuleIf(DruidConfig.class, predicate, module));
    }

    private static Module noneAuthenticationModule(DruidConfig druidConfig)
    {
        return binder -> httpClientBinder(binder).bindHttpClient("druid-client", ForDruidClient.class)
                .withConfigDefaults(config -> applyTlsConfig(config, druidConfig));
    }

    private static Module basicAuthenticationModule(DruidConfig druidConfig)
    {
        return binder -> httpClientBinder(binder).bindHttpClient("druid-client", ForDruidClient.class)
                .withConfigDefaults(
                        config -> {
                            config.setAuthenticationEnabled(false); //disable Kerberos auth
                            applyTlsConfig(config, druidConfig);
                        }
                ).withFilter(
                        DruidBasicAuthHttpRequestFilter.class);
    }

    private static Module kerberosbAuthenticationModule(DruidConfig druidConfig)
    {
        return binder -> httpClientBinder(binder).bindHttpClient("druid-client", ForDruidClient.class)
                .withConfigDefaults(
                        config -> {
                            config.setAuthenticationEnabled(true);
                            applyTlsConfig(config, druidConfig);
                        });
    }

    private static void applyTlsConfig(HttpClientConfig config, DruidConfig druidConfig)
    {
        if (druidConfig.isTlsEnabled()) {
            requireNonNull(druidConfig.getTrustStorePath(), "druid.tls.truststore-path is null");
            config.setTrustStorePath(druidConfig.getTrustStorePath());
            config.setTrustStorePassword(druidConfig.getTrustStorePassword()); // Not adding null check for truststore password as it can be null for self-signed certs
        }
    }
}
