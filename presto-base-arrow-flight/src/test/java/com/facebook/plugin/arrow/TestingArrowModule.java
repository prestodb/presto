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
package com.facebook.plugin.arrow;

import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class TestingArrowModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(TestingArrowFlightConfig.class);
        binder.bind(ConnectorSplitManager.class).to(TestingArrowSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(AbstractArrowFlightClientHandler.class).to(TestingArrowFlightClientHandler.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(TestingArrowMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ArrowBlockBuilder.class).to(TestingArrowBlockBuilder.class).in(Scopes.SINGLETON);
    }
}
