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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

public class TableFinishInfo
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String connectorOutputMetadata = null;

    public TableFinishInfo(Optional<ConnectorOutputMetadata> connectorOutputMetadata)
    {
        try {
            if (connectorOutputMetadata.isPresent()) {
                this.connectorOutputMetadata = MAPPER.writeValueAsString(connectorOutputMetadata.get().getInfo());
            }
        }
        catch (JsonProcessingException ignored) {
        }
    }

    @JsonCreator
    public TableFinishInfo(@JsonProperty("connectorOutputMetadata") JsonNode connectorOutputMetadata)
            throws JsonProcessingException
    {
        this.connectorOutputMetadata = MAPPER.writeValueAsString(connectorOutputMetadata);
    }

    @JsonProperty
    @JsonRawValue
    public String getConnectorOutputMetadata()
    {
        return connectorOutputMetadata;
    }
}
