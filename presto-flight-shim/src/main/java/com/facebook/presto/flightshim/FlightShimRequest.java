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
package com.facebook.presto.flightshim;

import com.facebook.presto.common.type.RowType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FlightShimRequest
{
    private final String connectorId;
    private final RowType outputType;
    private final byte[] splitBytes;
    private final List<byte[]> columnHandlesBytes;
    private final byte[] tableHandleBytes;
    private final Optional<byte[]> tableLayoutHandleBytes;
    private final byte[] transactionHandleBytes;

    @JsonCreator
    public FlightShimRequest(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("outputType") RowType outputType,
            @JsonProperty("splitBytes") byte[] splitBytes,
            @JsonProperty("columnHandlesBytes") List<byte[]> columnHandlesBytes,
            @JsonProperty("tableHandleBytes") byte[] tableHandleBytes,
            @JsonProperty("tableLayoutHandleBytes") Optional<byte[]> tableLayoutHandleBytes,
            @JsonProperty("transactionHandleBytes") byte[] transactionHandleBytes)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.splitBytes = requireNonNull(splitBytes, "splitBytes is null");
        this.columnHandlesBytes = ImmutableList.copyOf(requireNonNull(columnHandlesBytes, "columnHandlesBytes is null"));
        this.tableHandleBytes = requireNonNull(tableHandleBytes, "tableHandleBytes is null");
        this.tableLayoutHandleBytes = tableLayoutHandleBytes;
        this.transactionHandleBytes = requireNonNull(transactionHandleBytes, "transactionHandleBytes is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public RowType getOutputType()
    {
        return outputType;
    }

    @JsonProperty
    public byte[] getSplitBytes()
    {
        return splitBytes;
    }

    @JsonProperty
    public List<byte[]> getColumnHandlesBytes()
    {
        return columnHandlesBytes;
    }

    @JsonProperty
    public byte[] getTableHandleBytes()
    {
        return tableHandleBytes;
    }

    @JsonProperty
    public Optional<byte[]> getTableLayoutHandleBytes()
    {
        return tableLayoutHandleBytes;
    }

    @JsonProperty
    public byte[] getTransactionHandleBytes()
    {
        return transactionHandleBytes;
    }
}
