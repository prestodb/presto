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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.net.URI;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class LocalFileTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final LocalFileConnectorId connectorId;
    private final URI source;

    @JsonCreator
    public LocalFileTableHandle(
            @JsonProperty("connectorId") LocalFileConnectorId connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("source") URI source)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.source = requireNonNull(source, "source is null");
    }

    @JsonProperty
    public LocalFileConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public URI getSource()
    {
        return source;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalFileTableHandle that = (LocalFileTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(source, that.source);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, connectorId, source);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(connectorId, schemaTableName);
    }
}
