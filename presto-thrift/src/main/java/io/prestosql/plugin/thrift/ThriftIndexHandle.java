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
package io.prestosql.plugin.thrift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorIndexHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ThriftIndexHandle
        implements ConnectorIndexHandle
{
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<ConnectorSession> session;

    public ThriftIndexHandle(
            SchemaTableName schemaTableName,
            TupleDomain<ColumnHandle> tupleDomain,
            ConnectorSession session)
    {
        this(schemaTableName, tupleDomain, Optional.of(requireNonNull(session, "session is null")));
    }

    @JsonCreator
    public ThriftIndexHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this(schemaTableName, tupleDomain, Optional.empty());
    }

    private ThriftIndexHandle(
            SchemaTableName schemaTableName,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ConnectorSession> session)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.session = requireNonNull(session, "session is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
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
        ThriftIndexHandle other = (ThriftIndexHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.tupleDomain, other.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, tupleDomain);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString() +
                session.map(value -> ", constraint = " + tupleDomain.toString(value)).orElse("");
    }
}
