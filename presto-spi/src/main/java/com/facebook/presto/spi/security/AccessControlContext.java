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
package com.facebook.presto.spi.security;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AccessControlContext
{
    private final QueryId queryId;
    private final Optional<String> clientInfo;
    private final Optional<String> source;
    private final WarningCollector warningCollector;
    private final RuntimeStats runtimeStats;

    public AccessControlContext(QueryId queryId, Optional<String> clientInfo, Optional<String> source, WarningCollector warningCollector, RuntimeStats runtimeStats)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.source = requireNonNull(source, "source is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public WarningCollector getWarningCollector()
    {
        return warningCollector;
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
