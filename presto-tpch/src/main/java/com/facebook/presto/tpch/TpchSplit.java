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
package com.facebook.presto.tpch;

import com.facebook.presto.common.experimental.ColumnHandleAdapter;
import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.ThriftTupleDomainSerde;
import com.facebook.presto.common.experimental.auto_gen.ThriftColumnHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorSplit;
import com.facebook.presto.common.experimental.auto_gen.ThriftTpchSplit;
import com.facebook.presto.common.experimental.auto_gen.ThriftTupleDomain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

// Right now, splits are just the entire TPCH table
public class TpchSplit
        implements ConnectorSplit
{
    private final TpchTableHandle tableHandle;
    private final int totalParts;
    private final int partNumber;
    private final List<HostAddress> addresses;
    private final TupleDomain<ColumnHandle> predicate;

    static {
        ThriftSerializationRegistry.registerSerializer(TpchSplit.class, TpchSplit::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(TpchSplit.class, ThriftTpchSplit.class, TpchSplit::deserialize, null);
    }

    public TpchSplit(ThriftTpchSplit thriftTpchSplit)
    {
        this(new TpchTableHandle(thriftTpchSplit.getTableHandle()),
                thriftTpchSplit.getPartNumber(),
                thriftTpchSplit.getTotalParts(),
                thriftTpchSplit.getAddresses().stream().map(HostAddress::new).collect(Collectors.toList()),
                TupleDomain.fromThrift(thriftTpchSplit.getPredicate(), new ThriftTupleDomainSerde<ColumnHandle>()
                {
                    @Override
                    public ColumnHandle deserialize(byte[] bytes)
                    {
                        return (ColumnHandle) ColumnHandleAdapter.fromThrift(FbThriftUtils.deserialize(ThriftColumnHandle.class, bytes));
                    }
                }));
    }

    @JsonCreator
    public TpchSplit(@JsonProperty("tableHandle") TpchTableHandle tableHandle,
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalParts") int totalParts,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalParts >= 1, "totalParts must be >= 1");
        checkState(totalParts > partNumber, "totalParts must be > partNumber");

        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partNumber = partNumber;
        this.totalParts = totalParts;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public TpchTableHandle getTableHandle()
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
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
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
        TpchSplit other = (TpchSplit) obj;
        return Objects.equals(this.tableHandle, other.tableHandle) &&
                Objects.equals(this.totalParts, other.totalParts) &&
                Objects.equals(this.partNumber, other.partNumber) &&
                Objects.equals(this.predicate, other.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, totalParts, partNumber);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("partNumber", partNumber)
                .add("totalParts", totalParts)
                .add("predicate", predicate)
                .toString();
    }

    @Override
    public ThriftConnectorSplit toThriftInterface()
    {
        return ThriftConnectorSplit.builder()
                .setType(getImplementationType())
                .setSerializedSplit(FbThriftUtils.serialize(this.toThrift()))
                .build();
    }

    @Override
    public ThriftTpchSplit toThrift()
    {
        ThriftTupleDomain thriftTupleDomain;
        if (predicate.isAll()) {
            thriftTupleDomain = new ThriftTupleDomain("DummyKeyClassName", null);
        }
        else {
            thriftTupleDomain = predicate.toThrift(new ThriftTupleDomainSerde<ColumnHandle>()
            {
                @Override
                public byte[] serialize(ColumnHandle obj)
                {
                    return FbThriftUtils.serialize(ThriftColumnHandle.builder()
                            .setType(obj.getImplementationType())
                            .setSerializedHandle(FbThriftUtils.serialize(obj.toThrift()))
                            .build());
                }
            });
        }

        ThriftTpchSplit thriftSplit = new ThriftTpchSplit(
                tableHandle.toThrift(),
                totalParts,
                partNumber,
                addresses.stream().map(HostAddress::toThrift).collect(Collectors.toList()),
                thriftTupleDomain);
        return thriftSplit;
    }

    public static TpchSplit deserialize(byte[] bytes)
    {
        return new TpchSplit(FbThriftUtils.deserialize(ThriftTpchSplit.class, bytes));
    }
}
