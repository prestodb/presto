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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.raptorx.metadata.ChunkRecorder;
import com.facebook.presto.raptorx.metadata.DatabaseChunkManager;
import com.facebook.presto.raptorx.metadata.DatabaseChunkRecorder;
import com.facebook.presto.raptorx.metadata.SequenceManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

import javax.inject.Singleton;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class StorageModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StorageConfig.class);
        configBinder(binder).bindConfig(ChunkRecoveryConfig.class);

        binder.bind(StorageManager.class).to(OrcStorageManager.class).in(SINGLETON);
        binder.bind(StorageService.class).to(FileStorageService.class).in(SINGLETON);
        binder.bind(ReaderAttributes.class).in(SINGLETON);
        binder.bind(ChunkRecoveryManager.class).asEagerSingleton();
        binder.bind(ChunkManager.class).to(DatabaseChunkManager.class).in(SINGLETON);
        binder.bind(ChunkRecorder.class).to(DatabaseChunkRecorder.class).in(SINGLETON);

        newExporter(binder).export(StorageManager.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public static ChunkIdSequence createChunkIdSequence(SequenceManager sequenceManager)
    {
        return () -> sequenceManager.nextValue("chunk_id", 100);
    }
}
