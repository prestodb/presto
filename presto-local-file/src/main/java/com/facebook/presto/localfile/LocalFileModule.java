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
package com.facebook.presto.localfile;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class LocalFileModule
        implements Module
{
    private final String connectorId;

    public LocalFileModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(LocalFileConfig.class);

        binder.bind(LocalFileConnector.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(LocalFileHandleResolver.class).in(Scopes.SINGLETON);

        binder.bind(LocalFileTables.class).in(Scopes.SINGLETON);
    }
}
