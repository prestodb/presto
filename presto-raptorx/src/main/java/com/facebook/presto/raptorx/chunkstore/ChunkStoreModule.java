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
package com.facebook.presto.raptorx.chunkstore;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Singleton;

import java.util.Map;

import static com.google.inject.Scopes.SINGLETON;
import static org.weakref.jmx.ObjectNames.generatedNameOf;

public class ChunkStoreModule
        extends AbstractConfigurationAwareModule
{
    private final Map<String, Module> providers;

    public ChunkStoreModule(Map<String, Module> providers)
    {
        this.providers = ImmutableMap.<String, Module>builder()
                .put("file", new FileChunkStoreModule())
                .putAll(providers)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ChunkStoreManager.class).in(SINGLETON);

        String provider = buildConfigObject(ChunkStoreConfig.class).getProvider();
        Module module = providers.get(provider);
        if (module == null) {
            binder.addError("Unknown chunk store provider: %s", provider);
        }
        else {
            install(module);
        }
    }

    @Provides
    @Singleton
    public static ChunkStore createChunkStore(
            @RawChunkStore ChunkStore store,
            LifeCycleManager lifeCycleManager,
            MBeanExporter exporter,
            ChunkStoreConfig config)
            throws Exception
    {
        ChunkStore proxy = new TimeoutChunkStore(store, config.getTimeout(), config.getTimeoutThreads());
        lifeCycleManager.addInstance(proxy);

        ChunkStore managed = new ManagedChunkStore(proxy);
        exporter.export(generatedNameOf(ChunkStore.class), managed);
        return managed;
    }
}
