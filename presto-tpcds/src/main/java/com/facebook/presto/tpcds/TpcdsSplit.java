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
package com.facebook.presto.tpcds;

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorSplit;
import com.facebook.presto.common.experimental.auto_gen.ThriftTpcdsSplit;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TpcdsSplit
        implements ConnectorSplit
{
    private final TpcdsTableHandle tableHandle;
    private final int totalParts;
    private final int partNumber;
    private final List<HostAddress> addresses;
    private final boolean noSexism;

    static {
        ThriftSerializationRegistry.registerSerializer(TpcdsSplit.class, TpcdsSplit::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(TpcdsSplit.class, ThriftTpcdsSplit.class, TpcdsSplit::deserialize, null);
    }

    public static TpcdsSplit createTpcdsSplit(ThriftTpcdsSplit thriftSplit)
    {
        return new TpcdsSplit(new TpcdsTableHandle(thriftSplit.getTableHandle()),
                thriftSplit.getPartNumber(),
                thriftSplit.getTotalParts(),
                thriftSplit.getAddresses().stream().map(HostAddress::new).collect(Collectors.toList()),
                thriftSplit.isNoSexism());
    }

    public ThriftTpcdsSplit toThrift()
    {
        return new ThriftTpcdsSplit(
                tableHandle.toThrift(),
                totalParts,
                partNumber,
                addresses.stream().map(HostAddress::toThrift).collect(Collectors.toList()),
                noSexism);
    }

    public ThriftConnectorSplit toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            ThriftConnectorSplit thriftSplit = new ThriftConnectorSplit();
            thriftSplit.setType(getImplementationType());
            thriftSplit.setSerializedSplit(serializer.serialize(this.toThrift()));
            return thriftSplit;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonCreator
    public TpcdsSplit(
            @JsonProperty("tableHandle") TpcdsTableHandle tableHandle,
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalParts") int totalParts,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("noSexism") boolean noSexism)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalParts >= 1, "totalParts must be >= 1");
        checkState(totalParts > partNumber, "totalParts must be > partNumber");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(addresses, "addresses is null");

        this.tableHandle = tableHandle;
        this.partNumber = partNumber;
        this.totalParts = totalParts;
        this.addresses = ImmutableList.copyOf(addresses);
        this.noSexism = noSexism;
    }

    @JsonProperty
    public TpcdsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public int getTotalParts()
    {
        return totalParts;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return HARD_AFFINITY;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return addresses;
    }

    @JsonProperty
    public boolean isNoSexism()
    {
        return noSexism;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TpcdsSplit other = (TpcdsSplit) obj;
        return Objects.equals(this.tableHandle, other.tableHandle) &&
                Objects.equals(this.totalParts, other.totalParts) &&
                Objects.equals(this.partNumber, other.partNumber) &&
                Objects.equals(this.noSexism, other.noSexism);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, totalParts, partNumber, noSexism);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("partNumber", partNumber)
                .add("totalParts", totalParts)
                .add("noSexism", noSexism)
                .toString();
    }

    public static TpcdsSplit deserialize(byte[] bytes)
    {
        try {
            ThriftTpcdsSplit thriftSplit = new ThriftTpcdsSplit();
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            deserializer.deserialize(thriftSplit, bytes);
            return createTpcdsSplit(thriftSplit);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
