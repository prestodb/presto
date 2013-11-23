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
package com.facebook.presto.example;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class ExampleModule
        implements Module
{
    private final String connectorId;

    public ExampleModule(String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ExampleConnectorId.class).toInstance(new ExampleConnectorId(connectorId));
        binder.bind(ExampleMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ExampleClient.class).in(Scopes.SINGLETON);
        binder.bind(ExampleSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ExampleRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ExampleHandleResolver.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(ExampleConfig.class);

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(ExampleTable.class));
    }
}
