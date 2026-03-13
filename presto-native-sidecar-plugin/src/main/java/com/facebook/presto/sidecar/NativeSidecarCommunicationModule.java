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
package com.facebook.presto.sidecar;

import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.presto.common.AuthClientConfigs;
import com.facebook.presto.server.InternalAuthenticationManager;
import com.facebook.presto.server.security.InternalAuthenticationFilter;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import jakarta.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.Objects.requireNonNull;

public class NativeSidecarCommunicationModule
        implements Module
{
    private final AuthClientConfigs authClientConfigs;
    public NativeSidecarCommunicationModule(AuthClientConfigs authClientConfigs)
    {
        this.authClientConfigs = requireNonNull(authClientConfigs, "authClientConfigs is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfigGlobalDefaults(HttpClientConfig.class, config -> {
            config.setKeyStorePath(authClientConfigs.getKeyStorePath());
            config.setKeyStorePassword(authClientConfigs.getKeyStorePassword());
            config.setTrustStorePath(authClientConfigs.getTrustStorePath());
            config.setTrustStorePassword(authClientConfigs.getTrustStorePassword());
            if (authClientConfigs.getIncludedCipherSuites().isPresent()) {
                config.setHttpsIncludedCipherSuites(authClientConfigs.getIncludedCipherSuites().get());
            }
            if (authClientConfigs.getExcludeCipherSuites().isPresent()) {
                config.setHttpsExcludedCipherSuites(authClientConfigs.getExcludeCipherSuites().get());
            }
        });

        binder.bind(AuthClientConfigs.class).toInstance(authClientConfigs);
        httpClientBinder(binder).bindGlobalFilter(InternalAuthenticationManager.class);
        jaxrsBinder(binder).bind(InternalAuthenticationFilter.class);
        httpClientBinder(binder).bindHttpClient("sidecar", ForSidecarInfo.class);
    }

    @Provides
    @Singleton
    public InternalAuthenticationManager provideInternalAuthenticationManager(
            AuthClientConfigs authClientConfigs)
    {
        return new InternalAuthenticationManager(
                authClientConfigs.getSharedSecret(),
                authClientConfigs.getNodeId(),
                authClientConfigs.isInternalJwtEnabled());
    }
}
