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
package com.facebook.presto;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.spi.ClientRequestFilter;
import com.facebook.presto.spi.ClientRequestFilterFactory;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientRequestFilterManager
{
    private final List<ClientRequestFilterFactory> factories = new CopyOnWriteArrayList<>();
    private volatile List<ClientRequestFilter> immutableFilters = ImmutableList.of();
    private static final Logger log = Logger.get(PluginManager.class);

    public void registerClientRequestFilterFactory(ClientRequestFilterFactory factory)
    {
        factories.add(factory);
        loadClientRequestFilters();
    }

    public void loadClientRequestFilters()
    {
        ImmutableList<ClientRequestFilter> filters = factories.stream()
                .map(factory -> {
                    try {
                        return factory.create(factory.getName());
                    }
                    catch (Exception e) {
                        log.error(e, "Failed to create filter for factory '%s' of type '%s'",
                                factory.getName(), factory.getClass().getName());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(ImmutableList.toImmutableList());

        immutableFilters = filters;
    }

    public List<ClientRequestFilter> getClientRequestFilters()
    {
        return immutableFilters;
    }
}
