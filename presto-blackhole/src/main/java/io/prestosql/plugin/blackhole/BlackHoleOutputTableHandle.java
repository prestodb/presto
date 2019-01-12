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
package io.prestosql.plugin.blackhole;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;

import static java.util.Objects.requireNonNull;

public final class BlackHoleOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final BlackHoleTableHandle table;
    private final Duration pageProcessingDelay;

    @JsonCreator
    public BlackHoleOutputTableHandle(
            @JsonProperty("table") BlackHoleTableHandle table,
            @JsonProperty("pageProcessingDelay") Duration pageProcessingDelay)
    {
        this.table = requireNonNull(table, "table is null");
        this.pageProcessingDelay = requireNonNull(pageProcessingDelay, "pageProcessingDelay is null");
    }

    @JsonProperty
    public BlackHoleTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Duration getPageProcessingDelay()
    {
        return pageProcessingDelay;
    }
}
