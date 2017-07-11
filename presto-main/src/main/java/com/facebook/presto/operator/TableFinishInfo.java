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

import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;

public class TableFinishInfo
        implements OperatorInfo
{
    private static final int JSON_LENGTH_LIMIT = toIntExact(new DataSize(10, MEGABYTE).toBytes());
    private static final JsonCodec<Object> INFO_CODEC = jsonCodec(Object.class);
    private static final JsonCodec<JsonNode> JSON_NODE_CODEC = jsonCodec(JsonNode.class);

    private String connectorOutputMetadata = null;
    private boolean jsonLengthLimitExceeded = false;

    public TableFinishInfo(Optional<ConnectorOutputMetadata> metadata)
    {
        if (metadata.isPresent()) {
            Optional<String> serializedMetadata = INFO_CODEC.toJsonWithLengthLimit(metadata.get().getInfo(), JSON_LENGTH_LIMIT);
            if (!serializedMetadata.isPresent()) {
                jsonLengthLimitExceeded = true;
            }
            else {
                connectorOutputMetadata = serializedMetadata.get();
            }
        }
    }

    @JsonCreator
    public TableFinishInfo(@JsonProperty("connectorOutputMetadata") JsonNode connectorOutputMetadata)
            throws JsonProcessingException
    {
        this.connectorOutputMetadata = JSON_NODE_CODEC.toJson(connectorOutputMetadata);
    }

    @Override
    public boolean isFinal()
    {
        return true;
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
}
