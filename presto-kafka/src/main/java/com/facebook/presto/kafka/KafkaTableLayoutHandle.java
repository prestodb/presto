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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class KafkaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KafkaTableHandle table;
    private final long startOffsetTimestamp;
    private final long endOffsetTimestamp;

    @JsonCreator
    public KafkaTableLayoutHandle(
            @JsonProperty("table") KafkaTableHandle table,
            @JsonProperty("startOffsetTimestamp") long startOffsetTimestamp,
            @JsonProperty("endOffsetTimestamp") long endOffsetTimestamp)
    {
        this.table = requireNonNull(table, "table is null");
        this.startOffsetTimestamp = startOffsetTimestamp;
        this.endOffsetTimestamp = endOffsetTimestamp;
    }

    @JsonProperty
    public KafkaTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public long getStartOffsetTimestamp()
    {
        return startOffsetTimestamp;
    }

    @JsonProperty
    public long getEndOffsetTimestamp()
    {
        return endOffsetTimestamp;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table.toString())
                .add("startOffsetTimestamp", startOffsetTimestamp)
                .add("endOffsetTimestamp", endOffsetTimestamp).toString();
    }
}
