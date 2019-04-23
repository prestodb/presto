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
import com.facebook.presto.raptorx.metadata.WorkerTransactionCleanerConfig;
import com.facebook.presto.raptorx.metadata.WorkerTransactionCleanerJob;
import com.facebook.presto.raptorx.storage.organization.ChunkCompactionManager;
import com.facebook.presto.raptorx.storage.organization.ChunkCompactor;
import com.facebook.presto.raptorx.storage.organization.ChunkOrganizationManager;
import com.facebook.presto.raptorx.storage.organization.ChunkOrganizer;
import com.facebook.presto.raptorx.storage.organization.JobFactory;
import com.facebook.presto.raptorx.storage.organization.OrganizationJobFactory;
import com.facebook.presto.raptorx.storage.organization.TemporalFunction;
import com.google.common.base.Ticker;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class StorageModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(Ticker.class).toInstance(Ticker.systemTicker());

        configBinder(binder).bindConfig(StorageConfig.class);
        configBinder(binder).bindConfig(ChunkRecoveryConfig.class);
        configBinder(binder).bindConfig(LocalCleanerConfig.class);

        binder.bind(StorageManager.class).to(OrcStorageManager.class).in(SINGLETON);
        binder.bind(StorageService.class).to(FileStorageService.class).in(SINGLETON);
        binder.bind(ReaderAttributes.class).in(SINGLETON);
        binder.bind(ChunkRecoveryManager.class).asEagerSingleton();
        binder.bind(ChunkManager.class).to(DatabaseChunkManager.class).in(SINGLETON);
        binder.bind(ChunkRecorder.class).to(DatabaseChunkRecorder.class).in(SINGLETON);
        binder.bind(LocalCleaner.class).in(Scopes.SINGLETON);
        binder.bind(ChunkCompactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ChunkCompactor.class).in(Scopes.SINGLETON);
        binder.bind(TemporalFunction.class).in(Scopes.SINGLETON);
        binder.bind(ChunkOrganizationManager.class).in(Scopes.SINGLETON);
        binder.bind(ChunkOrganizer.class).in(Scopes.SINGLETON);
        binder.bind(JobFactory.class).to(OrganizationJobFactory.class).in(Scopes.SINGLETON);

        // For cleaning worker's garbage in db and backup store.
        configBinder(binder).bindConfig(WorkerTransactionCleanerConfig.class);
        binder.bind(WorkerTransactionCleanerJob.class).in(SINGLETON);
    }

    @Provides
    @Singleton
    public static ChunkIdSequence createChunkIdSequence(SequenceManager sequenceManager, StorageConfig storageConfig)
    {
        return () -> sequenceManager.nextValue("chunk_id", storageConfig.getChunkSequenceCacheCount());
    }
}
