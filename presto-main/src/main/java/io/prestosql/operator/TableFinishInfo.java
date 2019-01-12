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
package io.prestosql.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.ConnectorOutputMetadata;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TableFinishInfo
        implements OperatorInfo
{
    private static final int JSON_LENGTH_LIMIT = toIntExact(new DataSize(10, MEGABYTE).toBytes());
    private static final JsonCodec<Object> INFO_CODEC = jsonCodec(Object.class);
    private static final JsonCodec<JsonNode> JSON_NODE_CODEC = jsonCodec(JsonNode.class);

    private final String connectorOutputMetadata;
    private final boolean jsonLengthLimitExceeded;
    private final Duration statisticsWallTime;
    private final Duration statisticsCpuTime;

    public TableFinishInfo(Optional<ConnectorOutputMetadata> metadata, Duration statisticsWallTime, Duration statisticsCpuTime)
    {
        String connectorOutputMetadata = null;
        boolean jsonLengthLimitExceeded = false;
        if (metadata.isPresent()) {
            Optional<String> serializedMetadata = INFO_CODEC.toJsonWithLengthLimit(metadata.get().getInfo(), JSON_LENGTH_LIMIT);
            if (!serializedMetadata.isPresent()) {
                connectorOutputMetadata = null;
                jsonLengthLimitExceeded = true;
            }
            else {
                connectorOutputMetadata = serializedMetadata.get();
                jsonLengthLimitExceeded = false;
            }
        }
        this.connectorOutputMetadata = connectorOutputMetadata;
        this.jsonLengthLimitExceeded = jsonLengthLimitExceeded;
        this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
        this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
    }

    @JsonCreator
    public TableFinishInfo(
            @JsonProperty("connectorOutputMetadata") JsonNode connectorOutputMetadata,
            @JsonProperty("jsonLengthLimitExceeded") boolean jsonLengthLimitExceeded,
            @JsonProperty("statisticsWallTime") Duration statisticsWallTime,
            @JsonProperty("statisticsCpuTime") Duration statisticsCpuTime)
    {
        this.connectorOutputMetadata = JSON_NODE_CODEC.toJson(connectorOutputMetadata);
        this.jsonLengthLimitExceeded = jsonLengthLimitExceeded;
        this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
        this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
    }

    @JsonProperty
    @JsonRawValue
    public String getConnectorOutputMetadata()
    {
        return connectorOutputMetadata;
    }

    @JsonProperty
    public boolean isJsonLengthLimitExceeded()
    {
        return jsonLengthLimitExceeded;
    }

    @JsonProperty
    public Duration getStatisticsWallTime()
    {
        return statisticsWallTime;
    }

    @JsonProperty
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
