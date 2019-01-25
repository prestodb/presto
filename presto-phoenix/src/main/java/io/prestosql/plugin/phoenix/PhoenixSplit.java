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
package io.prestosql.plugin.phoenix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PhoenixSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final List<HostAddress> addresses;
    private final WrappedPhoenixInputSplit phoenixInputSplit;

    @JsonCreator
    public PhoenixSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("phoenixInputSplit") WrappedPhoenixInputSplit wrappedPhoenixInputSplit)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.addresses = addresses;
        this.phoenixInputSplit = wrappedPhoenixInputSplit;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty("phoenixInputSplit")
    public WrappedPhoenixInputSplit getWrappedPhoenixInputSplit()
    {
        return phoenixInputSplit;
    }

    @JsonIgnore
    public PhoenixInputSplit getPhoenixInputSplit()
    {
        return phoenixInputSplit.getPhoenixInputSplit();
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("hosts", addresses)
                .put("schema", schemaName)
                .put("table", tableName)
                .put("keyRange", getPhoenixInputSplit().getKeyRange())
                .build();
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
                addresses,
                phoenixInputSplit);
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
        PhoenixSplit other = (PhoenixSplit) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.tupleDomain, other.tupleDomain) &&
                Objects.equals(this.addresses, other.addresses) &&
                Objects.equals(this.phoenixInputSplit, other.phoenixInputSplit);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("connectorId", connectorId);
        if (catalogName != null) {
            helper.add("catalogName", catalogName);
        }
        if (schemaName != null) {
            helper.add("schemaName", schemaName);
        }

        helper.add("tableName", tableName)
                .add("tupleDomain", tupleDomain)
                .add("addresses", addresses)
                .add("phoenixInputSplit", getPhoenixInputSplit().getKeyRange());

        return helper.toString();
    }
}
