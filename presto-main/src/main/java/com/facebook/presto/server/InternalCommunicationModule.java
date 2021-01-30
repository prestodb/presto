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
package com.facebook.presto.server;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.airlift.http.client.spnego.KerberosConfig;
import com.google.inject.Binder;
import com.google.inject.Module;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.server.KerberosConfig.HTTP_SERVER_AUTHENTICATION_KRB5_KEYTAB;
import static com.facebook.presto.server.InternalCommunicationConfig.INTERNAL_COMMUNICATION_KERBEROS_ENABLED;
import static com.google.common.base.Verify.verify;

public class InternalCommunicationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        configBinder(binder).bindConfigGlobalDefaults(HttpClientConfig.class, config -> {
            config.setKeyStorePath(internalCommunicationConfig.getKeyStorePath());
            config.setKeyStorePassword(internalCommunicationConfig.getKeyStorePassword());
            config.setTrustStorePath(internalCommunicationConfig.getTrustStorePath());
            config.setTrustStorePassword(internalCommunicationConfig.getTrustStorePassword());
            if (internalCommunicationConfig.getIncludedCipherSuites().isPresent()) {
                config.setHttpsIncludedCipherSuites(internalCommunicationConfig.getIncludedCipherSuites().get());
            }
            if (internalCommunicationConfig.getExcludeCipherSuites().isPresent()) {
                config.setHttpsExcludedCipherSuites(internalCommunicationConfig.getExcludeCipherSuites().get());
            }
        });

        install(installModuleIf(InternalCommunicationConfig.class, InternalCommunicationConfig::isKerberosEnabled, kerberosInternalCommunicationModule()));
    }

    private Module kerberosInternalCommunicationModule()
    {
        return binder -> {
            InternalCommunicationConfig clientKerberosConfig = buildConfigObject(InternalCommunicationConfig.class);
            com.facebook.airlift.http.server.KerberosConfig serverKerberosConfig = buildConfigObject(com.facebook.airlift.http.server.KerberosConfig.class);
            verify(serverKerberosConfig.getKeytab() != null, "%s must be set when %s is true", HTTP_SERVER_AUTHENTICATION_KRB5_KEYTAB, INTERNAL_COMMUNICATION_KERBEROS_ENABLED);

            configBinder(binder).bindConfigGlobalDefaults(KerberosConfig.class, kerberosConfig -> {
                kerberosConfig.setConfig(serverKerberosConfig.getKerberosConfig());
                kerberosConfig.setKeytab(serverKerberosConfig.getKeytab());
                kerberosConfig.setUseCanonicalHostname(clientKerberosConfig.isKerberosUseCanonicalHostname());
            });

            String kerberosPrincipal = serverKerberosConfig.getServiceName() + "/" + getLocalCanonicalHostName();
            configBinder(binder).bindConfigGlobalDefaults(HttpClientConfig.class, httpClientConfig -> {
                httpClientConfig.setAuthenticationEnabled(true);
                httpClientConfig.setKerberosPrincipal(kerberosPrincipal);
                httpClientConfig.setKerberosRemoteServiceName(serverKerberosConfig.getServiceName());
            });
        };
    }

    private static String getLocalCanonicalHostName()
    {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
        }
        catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
