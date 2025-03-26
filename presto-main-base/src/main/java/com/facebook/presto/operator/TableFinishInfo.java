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
package com.facebook.presto.operator;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TableFinishInfo
        implements OperatorInfo
{
    private static final int JSON_LENGTH_LIMIT = toIntExact(new DataSize(10, MEGABYTE).toBytes());
    private static final JsonCodec<Object> INFO_CODEC = jsonCodec(Object.class);

    private final String serializedConnectorOutputMetadata;
    private final boolean jsonLengthLimitExceeded;
    private final Duration statisticsWallTime;
    private final Duration statisticsCpuTime;

    public TableFinishInfo(Optional<ConnectorOutputMetadata> metadata, Duration statisticsWallTime, Duration statisticsCpuTime)
    {
        String serializedConnectorOutputMetadata = null;
        boolean jsonLengthLimitExceeded = false;
        if (metadata.isPresent()) {
            Optional<String> serializedMetadata = INFO_CODEC.toJsonWithLengthLimit(metadata.get().getInfo(), JSON_LENGTH_LIMIT);
            if (!serializedMetadata.isPresent()) {
                serializedConnectorOutputMetadata = null;
                jsonLengthLimitExceeded = true;
            }
            else {
                serializedConnectorOutputMetadata = serializedMetadata.get();
                jsonLengthLimitExceeded = false;
            }
        }
        this.serializedConnectorOutputMetadata = serializedConnectorOutputMetadata;
        this.jsonLengthLimitExceeded = jsonLengthLimitExceeded;
        this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
        this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
    }

    @JsonCreator
    @ThriftConstructor
    public TableFinishInfo(
            @JsonProperty("serializedConnectorOutputMetadata") String serializedConnectorOutputMetadata,
            @JsonProperty("jsonLengthLimitExceeded") boolean jsonLengthLimitExceeded,
            @JsonProperty("statisticsWallTime") Duration statisticsWallTime,
            @JsonProperty("statisticsCpuTime") Duration statisticsCpuTime)
    {
        this.serializedConnectorOutputMetadata = serializedConnectorOutputMetadata;
        this.jsonLengthLimitExceeded = jsonLengthLimitExceeded;
        this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
        this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
    }

    @JsonProperty
    @ThriftField(1)
    public String getSerializedConnectorOutputMetadata()
    {
        return serializedConnectorOutputMetadata;
    }

    @JsonProperty
    @ThriftField(2)
    public boolean isJsonLengthLimitExceeded()
    {
        return jsonLengthLimitExceeded;
    }

    @JsonProperty
    @ThriftField(3)
    public Duration getStatisticsWallTime()
    {
        return statisticsWallTime;
    }

    @JsonProperty
    @ThriftField(4)
    public Duration getStatisticsCpuTime()
    {
        return statisticsCpuTime;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }
}
