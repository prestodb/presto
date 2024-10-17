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

import com.facebook.presto.spi.ClientRequestFilter;
import com.facebook.presto.spi.ClientRequestFilterFactory;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ClientRequestFilterManager
{
    private final List<ClientRequestFilterFactory> factories = new CopyOnWriteArrayList<>();
    private volatile List<ClientRequestFilter> filters = ImmutableList.of(); // Renamed from immutableFilters
    private boolean loadCalled;

    public void registerClientRequestFilterFactory(ClientRequestFilterFactory factory)
    {
        factories.add(factory);
    }

    public void loadClientRequestFilters()
    {
        synchronized (this) {
            if (loadCalled) {
                throw new PrestoException(CONFIGURATION_INVALID, "loadClientRequestFilters can only be called once.");
            }
            loadCalled = true;
        }

        filters = factories.stream()
                .map(factory -> factory.create(factory.getName()))
                .collect(toImmutableList());
    }

    public List<ClientRequestFilter> getClientRequestFilters()
    {
        return filters;
    }
}
