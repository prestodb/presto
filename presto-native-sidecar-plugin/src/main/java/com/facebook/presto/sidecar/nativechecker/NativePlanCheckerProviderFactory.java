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
package com.facebook.presto.sidecar.nativechecker;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.sidecar.NativeSidecarCommunicationModule;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanCheckerProviderContext;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.google.inject.Injector;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class NativePlanCheckerProviderFactory
        implements PlanCheckerProviderFactory
{
    private final ClassLoader classLoader;

    public NativePlanCheckerProviderFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "native";
    }

    @Override
    public PlanCheckerProvider create(Map<String, String> properties, PlanCheckerProviderContext context)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new NativePlanCheckerModule(context.getNodeManager(), context.getSimplePlanFragmentSerde()),
                    new NativeSidecarCommunicationModule());

            Injector injector = app
                    .noStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(properties)
                    .quiet()
                    .initialize();

            return injector.getInstance(PlanCheckerProvider.class);
        }
    }
}
