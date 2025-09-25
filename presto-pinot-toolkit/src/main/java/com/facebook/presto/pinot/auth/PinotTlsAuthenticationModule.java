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
package com.facebook.presto.pinot.auth;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.pinot.ForPinot;
import com.facebook.presto.pinot.PinotConfig;
import com.google.inject.Binder;

import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PinotTlsAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        PinotConfig pinotConfig = buildConfigObject(PinotConfig.class);

        httpClientBinder(binder)
                .bindHttpClient("pinot", ForPinot.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(300, SECONDS));
                    config.setRequestTimeout(new Duration(300, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                    config.setMaxContentLength(new DataSize(32, MEGABYTE));

                    // conditional TLS settings
                    if (pinotConfig.isUseSecureConnection()) {
                        requireNonNull(pinotConfig.getGrpcTlsTrustStorePath(), "pinot.grpc-tls-trust-store-path is null");
                        config.setTrustStorePath(pinotConfig.getGrpcTlsTrustStorePath());
                        config.setTrustStorePassword(pinotConfig.getGrpcTlsTrustStorePassword());
                    }
                });
    }
}
