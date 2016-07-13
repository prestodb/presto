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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.backup.BackupManager;
import com.facebook.presto.raptor.metadata.AssignmentLimiter;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.DatabaseShardRecorder;
import com.facebook.presto.raptor.metadata.MetadataConfig;
import com.facebook.presto.raptor.metadata.ShardCleaner;
import com.facebook.presto.raptor.metadata.ShardCleanerConfig;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardRecorder;
import com.facebook.presto.raptor.storage.organization.JobFactory;
import com.facebook.presto.raptor.storage.organization.OrganizationJobFactory;
import com.facebook.presto.raptor.storage.organization.ShardCompactionManager;
import com.facebook.presto.raptor.storage.organization.ShardCompactor;
import com.facebook.presto.raptor.storage.organization.ShardOrganizer;
import com.google.common.base.Ticker;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class StorageModule
        implements Module
{
    private final String connectorId;

    public StorageModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StorageManagerConfig.class);
        configBinder(binder).bindConfig(ShardCleanerConfig.class);
        configBinder(binder).bindConfig(MetadataConfig.class);

        binder.bind(Ticker.class).toInstance(Ticker.systemTicker());

        binder.bind(StorageManager.class).to(OrcStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(StorageService.class).to(FileStorageService.class).in(Scopes.SINGLETON);
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardRecorder.class).to(DatabaseShardRecorder.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseShardRecorder.class).in(Scopes.SINGLETON);
        binder.bind(ShardRecoveryManager.class).in(Scopes.SINGLETON);
        binder.bind(BackupManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardCompactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardOrganizer.class).in(Scopes.SINGLETON);
        binder.bind(JobFactory.class).to(OrganizationJobFactory.class).in(Scopes.SINGLETON);
        binder.bind(ShardCompactor.class).in(Scopes.SINGLETON);
        binder.bind(ShardEjector.class).in(Scopes.SINGLETON);
        binder.bind(ShardCleaner.class).in(Scopes.SINGLETON);
        binder.bind(ReaderAttributes.class).in(Scopes.SINGLETON);
        binder.bind(AssignmentLimiter.class).in(Scopes.SINGLETON);

        newExporter(binder).export(ShardRecoveryManager.class).as(generatedNameOf(ShardRecoveryManager.class, connectorId));
        newExporter(binder).export(BackupManager.class).as(generatedNameOf(BackupManager.class, connectorId));
        newExporter(binder).export(StorageManager.class).as(generatedNameOf(OrcStorageManager.class, connectorId));
        newExporter(binder).export(ShardCompactionManager.class).as(generatedNameOf(ShardCompactionManager.class, connectorId));
        newExporter(binder).export(ShardOrganizer.class).as(generatedNameOf(ShardOrganizer.class, connectorId));
        newExporter(binder).export(ShardCompactor.class).as(generatedNameOf(ShardCompactor.class, connectorId));
        newExporter(binder).export(ShardEjector.class).as(generatedNameOf(ShardEjector.class, connectorId));
        newExporter(binder).export(ShardCleaner.class).as(generatedNameOf(ShardCleaner.class, connectorId));
    }
}
