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
package com.facebook.presto.plugin.jdbc;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class JdbcSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<JdbcExpression> additionalPredicate;

    @JsonCreator
    public JdbcSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("additionalProperty") Optional<JdbcExpression> additionalPredicate)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
    }

    @ThriftConstructor
    public JdbcSplit(
            String connectorId,
            @Nullable String catalogName,
            @Nullable String schemaName,
            String tableName,
            JdbcThriftTupleDomain thriftTupleDomain,
            Optional<JdbcExpression> additionalPredicate)
    {
        this(connectorId, catalogName, schemaName, tableName, thriftTupleDomain.getTupleDomain().transform(columnHandle -> (ColumnHandle) columnHandle), additionalPredicate);
    }

    @JsonProperty
    @ThriftField(1)
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @ThriftField(2)
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @ThriftField(3)
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    @ThriftField(4)
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @ThriftField(value = 5, name = "tupleDomain")
    public JdbcThriftTupleDomain getThriftTupleDomain()
    {
        return new JdbcThriftTupleDomain(tupleDomain.transform(columnHandle -> (JdbcColumnHandle) columnHandle));
    }

    @JsonProperty
    @ThriftField(value = 6, name = "additionalProperty")
    public Optional<JdbcExpression> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                catalogName,
                schemaName,
                tableName,
                tupleDomain,
                additionalPredicate);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JdbcSplit other = (JdbcSplit) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.tupleDomain, other.tupleDomain) &&
                Objects.equals(this.additionalPredicate, other.additionalPredicate);
    }
}
