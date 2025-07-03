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
package com.facebook.presto.tvf;

import com.facebook.presto.spi.NodeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

import static java.util.Objects.requireNonNull;

public class NativeTVFProviderModule
        implements Module
{
    private final String catalogName;
    private final NodeManager nodeManager;

    public NativeTVFProviderModule(String catalogName, NodeManager nodeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(new TypeLiteral<String>() {}).annotatedWith(ServingCatalog.class).toInstance(catalogName);

        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(NativeTVFProvider.class).in(Scopes.SINGLETON);
    }
}
