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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.presto.spark.execution.property.NativeWorkerConfiguration;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkLocalShuffleInfoTranslator;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkShuffleInfoTranslator;
import com.facebook.presto.spark.execution.task.ForNativeExecutionTask;
import com.facebook.presto.spark.execution.task.NativeExecutionTaskFactory;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.units.Duration;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NativeExecutionModule
        implements Module
{
    private Map<String, Map<String, String>> connectorConfigs;
    private Map<String, String> configProperties;
    private Map<String, String> nodeConfigs;

    // For use by production system where the configurations can only be tuned via configurations.
    public NativeExecutionModule()
    {
        this.connectorConfigs = ImmutableMap.of();
    }

    // In the future, we would make more bindings injected into NativeExecutionModule
    // to be able to test various configuration parameters
    public NativeExecutionModule(Map<String, Map<String, String>> connectorConfigs, Map<String, String> configProperties)
    {
        this.connectorConfigs = connectorConfigs;
        this.configProperties = configProperties;
    }

    @Override
    public void configure(Binder binder)
    {
        bindWorkerProperties(binder);
        bindNativeExecutionTaskFactory(binder);
        bindHttpClient(binder);
        bindNativeExecutionProcess(binder);
        bindShuffle(binder);
    }

    protected void bindShuffle(Binder binder)
    {
        binder.bind(PrestoSparkLocalShuffleInfoTranslator.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, new TypeLiteral<PrestoSparkShuffleInfoTranslator>() {}).setDefault().to(PrestoSparkLocalShuffleInfoTranslator.class).in(Scopes.SINGLETON);
    }

    protected void bindWorkerProperties(Binder binder)
    {
        if (!connectorConfigs.isEmpty()) {
            binder.bind(NativeWorkerConfiguration.class).toInstance(createNativeWorkerConfiguration(connectorConfigs, configProperties));
        }
        else {
            binder.bind(NativeWorkerConfiguration.class).in(Scopes.SINGLETON);
        }
    }

    private NativeWorkerConfiguration createNativeWorkerConfiguration(Map<String, Map<String, String>> connectorConfigs, Map<String, String> configProperties)
    {
        // create node configs
        Map<String, String> nodeProperties = new HashMap<>();
        nodeProperties.put("node.environment", configProperties.getOrDefault("node.environment", "presto_cpp"));

        // set defaults for config.properties
        // create connector configs
        // Done
        return new NativeWorkerConfiguration(new HashMap<>(connectorConfigs), new HashMap<>(configProperties), new HashMap<>(nodeProperties));
    }

    protected void bindHttpClient(Binder binder)
    {
        httpClientBinder(binder)
                .bindHttpClient("nativeExecution", ForNativeExecutionTask.class)
                .withConfigDefaults(config -> {
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                });
    }

    protected void bindNativeExecutionTaskFactory(Binder binder)
    {
        binder.bind(NativeExecutionTaskFactory.class).in(Scopes.SINGLETON);
    }

    protected void bindNativeExecutionProcess(Binder binder)
    {
        if (System.getProperty("NATIVE_PORT") != null) {
            binder.bind(NativeExecutionProcessFactory.class).to(DetachedNativeExecutionProcessFactory.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(NativeExecutionProcessFactory.class).in(Scopes.SINGLETON);
        }
    }
}
