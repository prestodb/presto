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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KafkaSplit
        implements ConnectorSplit
{
    private final String schemaName;
    private final String tableName;
    private final List<HostAddress> addresses;
    private final String entityName;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public KafkaSplit(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("entityName") String entityName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.entityName = requireNonNull(entityName, "entityName is null");
        this.tupleDomain = requireNonNull(tupleDomain, "Domain tuple can not be null");
        //FIXME :do we require this?
        this.addresses = null;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }
    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }
    @JsonProperty
    public String getEntityName()
    {
        return entityName;
    }
    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }
}
