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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.experimental.ColumnHandleAdapter;
import com.facebook.presto.common.experimental.RowExpressionAdapter;
import com.facebook.presto.common.experimental.ThriftSerializable;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.ThriftSerializer;
import com.facebook.presto.common.experimental.auto_gen.ThriftBaseHiveTableLayoutHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTableLayoutHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftRowExpression;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class BaseHiveTableLayoutHandle
        implements ConnectorTableLayoutHandle, ThriftSerializable
{
    private final List<BaseHiveColumnHandle> partitionColumns;
    private final TupleDomain<Subfield> domainPredicate;
    private final RowExpression remainingPredicate;
    private final boolean pushdownFilterEnabled;
    private final TupleDomain<ColumnHandle> partitionColumnPredicate;

    // coordinator-only properties
    private final Optional<List<HivePartition>> partitions;

    static {
        ThriftSerializationRegistry.registerSerializer(BaseHiveTableLayoutHandle.class, BaseHiveTableLayoutHandle::serialize);
        ThriftSerializationRegistry.registerDeserializer("BASE_LAYOUT_HANDLE", BaseHiveTableLayoutHandle::deserialize);
    }

    public BaseHiveTableLayoutHandle(ThriftBaseHiveTableLayoutHandle thriftHandle)
    {
        this(thriftHandle.partitionColumns.stream().map(BaseHiveColumnHandle::new).collect(Collectors.toList()),
                TupleDomain.fromThrift(thriftHandle.getDomainPredicate(), new ThriftSerializer<Subfield>()
                {
                    @Override
                    public byte[] serialize(Subfield obj)
                    {
                        return obj.serialize();
                    }

                    @Override
                    public Subfield deserialize(byte[] bytes)
                    {
                        return null;
                    }
                }),
                (RowExpression) RowExpressionAdapter.fromThrift(thriftHandle.getRemainingPredicate()),
                thriftHandle.isPushdownFilterEnabled(),
                TupleDomain.fromThrift(thriftHandle.getPartitionColumnPredicate(), new ThriftSerializer<ColumnHandle>()
                {
                    @Override
                    public byte[] serialize(ColumnHandle obj)
                    {
                        return ColumnHandleAdapter.serialize(obj);
                    }

                    @Override
                    public ColumnHandle deserialize(byte[] bytes)
                    {
                        return null;
                    }
                }),
                thriftHandle.getPartitions().map(partions -> partions.stream().map(HivePartition::new).collect(Collectors.toList())));
    }

    @Override
    public ThriftConnectorTableLayoutHandle toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            ThriftConnectorTableLayoutHandle thriftHandle = new ThriftConnectorTableLayoutHandle();
            thriftHandle.setType(getImplementationType());
            thriftHandle.setSerializedConnectorTableLayoutHandle(serializer.serialize(new ThriftBaseHiveTableLayoutHandle(
                    partitionColumns.stream().map(BaseHiveColumnHandle::toThriftInterface).collect(Collectors.toList()),
                    domainPredicate.toThrift(new ThriftSerializer<Subfield>()
                    {
                        @Override
                        public byte[] serialize(Subfield obj)
                        {
                            return obj.serialize();
                        }

                        @Override
                        public Subfield deserialize(byte[] bytes)
                        {
                            return null;
                        }
                    }),
                    (ThriftRowExpression) remainingPredicate.toThriftInterface(),
                    pushdownFilterEnabled,
                    partitionColumnPredicate.toThrift(new ThriftSerializer<ColumnHandle>()
                    {
                        @Override
                        public byte[] serialize(ColumnHandle obj)
                        {
                            return obj.serialize();
                        }

                        @Override
                        public ColumnHandle deserialize(byte[] bytes)
                        {
                            return null;
                        }
                    }))));
            return thriftHandle;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public BaseHiveTableLayoutHandle(
            List<BaseHiveColumnHandle> partitionColumns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            boolean pushdownFilterEnabled,
            TupleDomain<ColumnHandle> partitionColumnPredicate,
            Optional<List<HivePartition>> partitions)
    {
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.pushdownFilterEnabled = pushdownFilterEnabled;
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    @JsonProperty
    public List<BaseHiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public TupleDomain<Subfield> getDomainPredicate()
    {
        return domainPredicate;
    }

    @JsonProperty
    public RowExpression getRemainingPredicate()
    {
        return remainingPredicate;
    }

    @JsonProperty
    public boolean isPushdownFilterEnabled()
    {
        return pushdownFilterEnabled;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPartitionColumnPredicate()
    {
        return partitionColumnPredicate;
    }

    /**
     * Partitions are dropped when HiveTableLayoutHandle is serialized.
     *
     * @return list of partitions if available, {@code Optional.empty()} if dropped
     */
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return partitions;
    }

    @Override
    public String getImplementationType()
    {
        return "BASE_LAYOUT_HANDLE";
    }

    @Override
    public byte[] serialize()
    {
        try {
            ThriftBaseHiveTableLayoutHandle thriftHandle = new ThriftBaseHiveTableLayoutHandle(
                    this.partitionColumns.stream().map(BaseHiveColumnHandle::toThriftInterface).collect(Collectors.toList()),
                    this.domainPredicate.toThrift(new ThriftSerializer<Subfield>()
                    {
                        @Override
                        public byte[] serialize(Subfield obj)
                        {
                            return obj.serialize();
                        }

                        @Override
                        public Subfield deserialize(byte[] bytes)
                        {
                            return null;
                        }
                    }),
                    (ThriftRowExpression) this.remainingPredicate.toThriftInterface(),
                    this.pushdownFilterEnabled,
                    this.partitionColumnPredicate.toThrift(new ThriftSerializer<ColumnHandle>()
                    {
                        @Override
                        public byte[] serialize(ColumnHandle obj)
                        {
                            return obj.serialize();
                        }

                        @Override
                        public ColumnHandle deserialize(byte[] bytes)
                        {
                            return null;
                        }
                    }));

            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            return serializer.serialize(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static BaseHiveTableLayoutHandle deserialize(byte[] bytes)
    {
        try {
            ThriftBaseHiveTableLayoutHandle thriftHandle = new ThriftBaseHiveTableLayoutHandle();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(thriftHandle, bytes);
            return new BaseHiveTableLayoutHandle(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
