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
package com.facebook.presto.plancheckerproviders.nativechecker;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Scopes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Objects.requireNonNull;

public class NativePlanCheckerProviderFactory
        implements PlanCheckerProviderFactory
{
    private static final File NATIVE_PLAN_CHECKER_PROVIDER_CONFIG = new File("etc/plan-checker-providers/native-plan-checker.properties");
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
    public PlanCheckerProvider create(SimplePlanFragmentSerde simplePlanFragmentSerde, NodeManager nodeManager)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        configBinder(binder).bindConfig(NativePlanCheckerConfig.class, NativePlanCheckerConfig.CONFIG_PREFIX);
                        binder.install(new JsonModule());
                        binder.bind(NodeManager.class).toInstance(nodeManager);
                        binder.bind(SimplePlanFragmentSerde.class).toInstance(simplePlanFragmentSerde);
                        jsonBinder(binder).addSerializerBinding(SimplePlanFragment.class).to(SimplePlanFragmentSerializer.class).in(Scopes.SINGLETON);
                        jsonCodecBinder(binder).bindJsonCodec(SimplePlanFragment.class);
                        binder.bind(PlanCheckerProvider.class).to(NativePlanCheckerProvider.class).in(Scopes.SINGLETON);
                    });

            Injector injector = app
                    .noStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(getConfig())
                    .quiet()
                    .initialize();

            return injector.getInstance(PlanCheckerProvider.class);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> getConfig()
            throws IOException
    {
        if (NATIVE_PLAN_CHECKER_PROVIDER_CONFIG.exists()) {
            return loadProperties(NATIVE_PLAN_CHECKER_PROVIDER_CONFIG);
        }
        return ImmutableMap.of();
    }

    static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        return fromProperties(properties);
    }
}
