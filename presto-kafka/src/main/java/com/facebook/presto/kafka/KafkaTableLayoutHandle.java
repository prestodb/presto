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
    private final long offsetStartTs;
    private final long offsetEndTs;

    @JsonCreator
    public KafkaTableLayoutHandle(
            @JsonProperty("table") KafkaTableHandle table,
            @JsonProperty("offset_start_ts") Long offsetStartTs,
            @JsonProperty("offset_end_ts") Long offsetEndTs)
    {
        this.table = requireNonNull(table, "table is null");
        this.offsetStartTs = offsetStartTs == null ? 0 : offsetStartTs;
        this.offsetEndTs = offsetEndTs == null ? 0 : offsetEndTs;
    }

    @JsonProperty
    public KafkaTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public long getOffsetStartTs()
    {
        return offsetStartTs;
    }

    @JsonProperty
    public long getOffsetEndTs()
    {
        return offsetEndTs;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table.toString())
                .add("offset_start_ts", offsetStartTs)
                .add("offset_end_ts", offsetEndTs).toString();
    }
}
