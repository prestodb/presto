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
package com.facebook.presto.raptor.filesystem;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.configuration.ConfigurationAwareModule;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;

import java.util.Map;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class FileSystemModule
        extends AbstractConfigurationAwareModule
{
    private final Map<String, Module> providers;

    public FileSystemModule(Map<String, Module> providers)
    {
        this.providers = ImmutableMap.<String, Module>builder()
                .put("file", new LocalFileSystemModule())
                .put("hdfs", new HdfsModule())
                .putAll(providers)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(StorageManagerConfig.class);

        String fileSystemProvider = buildConfigObject(StorageManagerConfig.class).getFileSystemProvider();
        Module module = providers.get(fileSystemProvider);

        if (module == null) {
            binder.addError("Unsupported file system: %s", fileSystemProvider);
        }
        else if (module instanceof ConfigurationAwareModule) {
            install(module);
        }
        else {
            binder.install(module);
        }
    }
}
