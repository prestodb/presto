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
package com.facebook.presto.sidecar.sessionpropertyproviders;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.sidecar.NativeSidecarCommunicationModule;
import com.facebook.presto.spi.session.SessionPropertyContext;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.spi.session.WorkerSessionPropertyProviderFactory;
import com.google.inject.Injector;

import java.util.Map;

public class NativeSystemSessionPropertyProviderFactory
        implements WorkerSessionPropertyProviderFactory
{
    public static final String NAME = "native-worker";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public WorkerSessionPropertyProvider create(SessionPropertyContext context, Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                new NativeSystemSessionPropertyProviderModule(
                        context.getNodeManager(), context.getTypeManager()),
                new NativeSidecarCommunicationModule());

        Injector injector = app
                .doNotInitializeLogging()
                .noStrictConfig()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(NativeSystemSessionPropertyProvider.class);
    }
}
