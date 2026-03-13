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

import com.facebook.presto.common.AuthClientConfigs;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.presto.server.CommonInternalCommunicationModule.bindInternalAuth;
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
        bindInternalAuth(binder, authClientConfigs);
        httpClientBinder(binder).bindHttpClient("sidecar", ForSidecarInfo.class);
    }
}
