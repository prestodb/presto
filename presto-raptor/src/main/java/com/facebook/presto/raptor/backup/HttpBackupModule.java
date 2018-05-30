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
package com.facebook.presto.raptor.backup;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.node.NodeInfo;

import javax.inject.Singleton;

import java.net.URI;
import java.util.function.Supplier;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class HttpBackupModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HttpBackupConfig.class);
        binder.bind(BackupStore.class).to(HttpBackupStore.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("backup", ForHttpBackup.class);
    }

    @Provides
    @Singleton
    @ForHttpBackup
    public String createEnvironment(NodeInfo nodeInfo)
    {
        return nodeInfo.getEnvironment();
    }

    @Provides
    @Singleton
    @ForHttpBackup
    public Supplier<URI> createBackupUriSupplier(HttpBackupConfig config)
    {
        URI uri = config.getUri();
        return () -> uri;
    }
}
