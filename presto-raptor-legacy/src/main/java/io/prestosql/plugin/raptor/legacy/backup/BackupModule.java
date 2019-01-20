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
package io.prestosql.plugin.raptor.legacy.backup;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.util.Providers;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationAwareModule;
import io.prestosql.plugin.raptor.legacy.RaptorConnectorId;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.Nullable;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class BackupModule
        extends AbstractConfigurationAwareModule
{
    private final Map<String, Module> providers;

    public BackupModule(Map<String, Module> providers)
    {
        this.providers = ImmutableMap.<String, Module>builder()
                .put("file", new FileBackupModule())
                .put("http", new HttpBackupModule())
                .putAll(providers)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(BackupConfig.class);

        String provider = buildConfigObject(BackupConfig.class).getProvider();
        if (provider == null) {
            binder.bind(BackupStore.class).toProvider(Providers.of(null));
        }
        else {
            Module module = providers.get(provider);
            if (module == null) {
                binder.addError("Unknown backup provider: %s", provider);
            }
            else if (module instanceof ConfigurationAwareModule) {
                install((ConfigurationAwareModule) module);
            }
            else {
                binder.install(module);
            }
        }
        binder.bind(BackupService.class).to(BackupServiceManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    private static Optional<BackupStore> createBackupStore(
            @Nullable BackupStore store,
            LifeCycleManager lifeCycleManager,
            MBeanExporter exporter,
            RaptorConnectorId connectorId,
            BackupConfig config)
            throws Exception
    {
        if (store == null) {
            return Optional.empty();
        }

        BackupStore proxy = new TimeoutBackupStore(
                store,
                connectorId.toString(),
                config.getTimeout(),
                config.getTimeoutThreads());

        lifeCycleManager.addInstance(proxy);

        BackupStore managed = new ManagedBackupStore(proxy);
        exporter.exportWithGeneratedName(managed, BackupStore.class, connectorId.toString());

        return Optional.of(managed);
    }
}
