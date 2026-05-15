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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.builtin.tools.WorkerFunctionRegistryTool;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import okhttp3.OkHttpClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;

/**
 * Guice module that wires the driver-side metadata sidecar.
 *
 * <p>Install only on the Presto-on-Spark driver (gated by {@code SparkProcessType == DRIVER} at the
 * call site, typically inside {@code PrestoFacebookSparkServiceFactory.getAdditionalModules}).
 * No-ops on executors.
 *
 * <p>Bindings:
 * <ul>
 *   <li>{@link MetadataSidecarConfig} — Airlift @Config
 *   <li>{@link MetadataSidecarProcessFactory} — singleton owning the sidecar lifecycle
 *   <li>{@link WorkerFunctionRegistryTool} → {@link DriverSidecarFunctionRegistryTool}
 *   <li>{@link OkHttpClient} / {@link ScheduledExecutorService} / {@link ExecutorService}
 *       all annotated {@link ForMetadataSidecar}, plus the JSON codecs the registry tool needs
 * </ul>
 *
 * <p>Callers may also supply their own binding for
 * {@link MetadataSidecarProcessFactory.SidecarBinaryLocator} to resolve the native worker
 * binary on disk when {@code metadata-sidecar.executable-path} is not set in config. This
 * module installs a default binding that returns {@code Optional.empty()} so the OSS build
 * does not depend on any deployment-specific launcher; deployments that want automatic
 * binary discovery should override the binding from their own module.
 */
public class DriverSidecarModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(MetadataSidecarConfig.class);
        binder.bind(MetadataSidecarProcessFactory.class).in(Scopes.SINGLETON);
        binder.bind(WorkerFunctionRegistryTool.class)
                .to(DriverSidecarFunctionRegistryTool.class)
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForMetadataSidecar
    public OkHttpClient provideOkHttpClient()
    {
        return new OkHttpClient.Builder()
                .connectTimeout(60, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(60, TimeUnit.SECONDS)
                .build();
    }

    @Provides
    @Singleton
    @ForMetadataSidecar
    public ScheduledExecutorService provideScheduledExecutor()
    {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metadata-sidecar-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    @Provides
    @Singleton
    @ForMetadataSidecar
    public ExecutorService provideExecutor()
    {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "metadata-sidecar-executor");
            t.setDaemon(true);
            return t;
        });
    }

    @Provides
    @Singleton
    public JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> provideFunctionSignatureMapJsonCodec()
    {
        return mapJsonCodec(String.class, listJsonCodec(JsonBasedUdfFunctionMetadata.class));
    }
}
