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
package com.facebook.presto.sessionpropertyproviders;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.session.SessionPropertyContext;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.facebook.presto.spi.session.SystemSessionPropertyProviderFactory;
import com.google.inject.Injector;
import com.google.inject.Scopes;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class NativeSystemSessionPropertyProviderFactory
        implements SystemSessionPropertyProviderFactory
{
    private final ClassLoader classLoader;

    public NativeSystemSessionPropertyProviderFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "native";
    }

    @Override
    public SystemSessionPropertyProvider create(Map<String, String> config, SessionPropertyContext context)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(SystemSessionPropertyProvider.class).to(NativeSystemSessionPropertyProvider.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .noStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(SystemSessionPropertyProvider.class);
        }
    }
}
