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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.google.common.collect.ImmutableList.toImmutableList;

@ThreadSafe
public class ClientRequestFilterManager
{
    private Map<String, ClientRequestFilterFactory> factories = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private volatile List<ClientRequestFilter> filters = ImmutableList.of();
    private final AtomicBoolean loaded = new AtomicBoolean();

    public void registerClientRequestFilterFactory(ClientRequestFilterFactory factory)
    {
        if (loaded.get()) {
            throw new PrestoException(INVALID_ARGUMENTS, "Cannot register factories after filters are loaded.");
        }

        String name = factory.getName();
        if (factories.putIfAbsent(name, factory) != null) {
            throw new PrestoException(ALREADY_EXISTS, "A factory with the name '" + name + "' is already registered.");
        }
    }

    public void loadClientRequestFilters()
    {
        if (!loaded.compareAndSet(false, true)) {
            throw new PrestoException(CONFIGURATION_INVALID, "loadClientRequestFilters can only be called once.");
        }

        filters = factories.values().stream()
                .map(factory -> factory.create())
                .collect(toImmutableList());
        factories = null;
    }

    public List<ClientRequestFilter> getClientRequestFilters()
    {
        return filters;
    }
}
