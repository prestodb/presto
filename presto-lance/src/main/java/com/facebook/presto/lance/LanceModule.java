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
package com.facebook.presto.lance;

import com.facebook.presto.lance.client.LanceClient;
import com.facebook.presto.lance.ingestion.LancePageWriter;
import com.facebook.presto.lance.metadata.LanceMetadata;
import com.facebook.presto.lance.ingestion.LancePageSinkProvider;
import com.facebook.presto.lance.scan.LancePageSourceProvider;
import com.facebook.presto.lance.splits.LanceSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class LanceModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(LanceConfig.class);
        binder.bind(LanceClient.class).in(Scopes.SINGLETON);
        binder.bind(LanceConnector.class).in(Scopes.SINGLETON);
        binder.bind(LanceMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LanceHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(LanceSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LancePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(LancePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(LancePageWriter.class).in(Scopes.SINGLETON);
    }
}
