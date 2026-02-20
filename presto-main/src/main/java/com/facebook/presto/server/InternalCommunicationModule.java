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
import com.facebook.presto.server.remotetask.ReactorNettyHttpClientConfig;
import com.google.inject.Binder;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class InternalCommunicationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        install(new CommonInternalCommunicationModule());

        configBinder(binder).bindConfigGlobalDefaults(ReactorNettyHttpClientConfig.class, config -> {
            config.setHttpsEnabled(internalCommunicationConfig.isHttpsRequired());
            config.setKeyStorePath(internalCommunicationConfig.getKeyStorePath());
            config.setKeyStorePassword(internalCommunicationConfig.getKeyStorePassword());
            config.setTrustStorePath(internalCommunicationConfig.getTrustStorePath());
            if (internalCommunicationConfig.getIncludedCipherSuites().isPresent()) {
                config.setCipherSuites(internalCommunicationConfig.getIncludedCipherSuites().get());
            }
        });
    }
}
