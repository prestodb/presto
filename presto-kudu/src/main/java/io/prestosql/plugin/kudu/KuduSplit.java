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
package io.prestosql.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KuduSplit
        implements ConnectorSplit
{
    private final KuduTableHandle tableHandle;
    private final int primaryKeyColumnCount;
    private final byte[] serializedScanToken;

    @JsonCreator
    public KuduSplit(@JsonProperty("tableHandle") KuduTableHandle tableHandle,
            @JsonProperty("primaryKeyColumnCount") int primaryKeyColumnCount,
            @JsonProperty("serializedScanToken") byte[] serializedScanToken)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.primaryKeyColumnCount = primaryKeyColumnCount;
        this.serializedScanToken = requireNonNull(serializedScanToken, "serializedScanToken is null");
    }

    @JsonProperty
    public KuduTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public byte[] getSerializedScanToken()
    {
        return serializedScanToken;
    }

    @JsonProperty
    public int getPrimaryKeyColumnCount()
    {
        return primaryKeyColumnCount;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
