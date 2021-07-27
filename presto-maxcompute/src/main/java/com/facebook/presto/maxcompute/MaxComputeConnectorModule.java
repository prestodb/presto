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
package com.facebook.presto.maxcompute;

import com.facebook.presto.metadata.ViewDefinition;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Guice module for the MaxCompute connector.
 */
public class MaxComputeConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MaxComputeConnector.class).in(Scopes.SINGLETON);
        binder.bind(MaxComputeClient.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MaxComputeClient.class).withGeneratedName();
        binder.bind(MaxComputeMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MaxComputeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MaxComputeRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(MaxComputePageSourceProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MaxComputePageSourceProvider.class).withGeneratedName();
        configBinder(binder).bindConfig(MaxComputeConfig.class);
        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);
        binder.bind(MaxComputeSessionProperties.class).in(Scopes.SINGLETON);
    }
}
