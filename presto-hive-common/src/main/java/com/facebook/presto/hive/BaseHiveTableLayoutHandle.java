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
import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.RowExpressionAdapter;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.ThriftTupleDomainSerde;
import com.facebook.presto.common.experimental.auto_gen.ThriftBaseHiveColumnHandle;
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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class BaseHiveTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final List<BaseHiveColumnHandle> partitionColumns;
    private final TupleDomain<Subfield> domainPredicate;
    private final RowExpression remainingPredicate;
    private final boolean pushdownFilterEnabled;
    private final TupleDomain<ColumnHandle> partitionColumnPredicate;

    // coordinator-only properties
    private final Optional<List<HivePartition>> partitions;

    static {
        ThriftSerializationRegistry.registerSerializer(BaseHiveTableLayoutHandle.class, BaseHiveTableLayoutHandle::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(BaseHiveTableLayoutHandle.class, ThriftBaseHiveTableLayoutHandle.class, BaseHiveTableLayoutHandle::deserialize, null);
    }

    public BaseHiveTableLayoutHandle(ThriftBaseHiveTableLayoutHandle thriftHandle)
    {
        this(thriftHandle.getPartitionColumns().stream().map(BaseHiveColumnHandle::new).collect(Collectors.toList()),
                TupleDomain.fromThrift(thriftHandle.getDomainPredicate(), new ThriftTupleDomainSerde<Subfield>()
                {
                    @Override
                    public Subfield deserialize(byte[] bytes)
                    {
                        return (Subfield) ThriftSerializationRegistry.deserialize(Subfield.class.getSimpleName(), bytes);
                    }
                }),
                (RowExpression) RowExpressionAdapter.fromThrift(thriftHandle.getRemainingPredicate()),
                thriftHandle.isPushdownFilterEnabled(),
                TupleDomain.fromThrift(thriftHandle.getPartitionColumnPredicate(), new ThriftTupleDomainSerde<ColumnHandle>()
                {
                    @Override
                    public ColumnHandle deserialize(byte[] bytes)
                    {
                        return (ColumnHandle) ColumnHandleAdapter.deserialize(bytes);
                    }
                }),
                Optional.ofNullable(thriftHandle.getPartitions()).map(partitions -> partitions.stream().map(HivePartition::new).collect(Collectors.toList())));
    }

    @Override
    public ThriftBaseHiveTableLayoutHandle toThrift()
    {
        return new ThriftBaseHiveTableLayoutHandle(
                partitionColumns.stream().map(column -> (ThriftBaseHiveColumnHandle) column.toThrift()).collect(Collectors.toList()),
                domainPredicate.toThrift(new ThriftTupleDomainSerde<Subfield>()
                {
                    @Override
                    public byte[] serialize(Subfield obj)
                    {
                        return ThriftSerializationRegistry.serialize(obj);
                    }
                }),
                (ThriftRowExpression) this.remainingPredicate.toThriftInterface(),
                this.pushdownFilterEnabled,
                this.partitionColumnPredicate.toThrift(new ThriftTupleDomainSerde<ColumnHandle>()
                {
                    @Override
                    public byte[] serialize(ColumnHandle obj)
                    {
                        return ColumnHandleAdapter.serialize(obj);
                    }
                }),
                // Partitions are oordinator-only properties
                null);
    }

    @Override
    public ThriftConnectorTableLayoutHandle toThriftInterface()
    {
        return new ThriftConnectorTableLayoutHandle(getImplementationType(), FbThriftUtils.serialize(this.toThrift()));
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

    public static BaseHiveTableLayoutHandle deserialize(byte[] bytes)
    {
        return new BaseHiveTableLayoutHandle(FbThriftUtils.deserialize(ThriftBaseHiveTableLayoutHandle.class, bytes));
    }
}
