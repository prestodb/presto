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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorPartitionKey
        implements PartitionKey
{
    private final String partitionName;
    private final String name;
    private final Type type;
    private final String value;

    @JsonCreator
    public RaptorPartitionKey(
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("value") String value)
    {
        checkNotNull(partitionName, "partitionName is null");
        checkNotNull(name, "name is null");
        checkNotNull(type, "type is null");
        checkNotNull(value, "value is null");

        this.partitionName = partitionName;
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("partitionName", partitionName)
                .add("name", name)
                .add("type", type)
                .add("value", value)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partitionName, name, type, value);
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
        final RaptorPartitionKey other = (RaptorPartitionKey) obj;
        return Objects.equal(this.partitionName, other.partitionName) &&
                Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.value, other.value);
    }

    public static class Mapper
            implements ResultSetMapper<RaptorPartitionKey>
    {
        private final TypeManager typeManager;

        @Inject
        public Mapper(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public RaptorPartitionKey map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new RaptorPartitionKey(r.getString("partition_name"),
                    r.getString("key_name"),
                    typeManager.getType(r.getString("key_type")),
                    r.getString("key_value"));
        }
    }

    public static Predicate<RaptorPartitionKey> partitionNamePredicate(final String partitionName)
    {
        checkNotNull(partitionName, "partitionName is null");

        return new Predicate<RaptorPartitionKey>()
        {
            @Override
            public boolean apply(RaptorPartitionKey input)
            {
                return partitionName.equals(input.getPartitionName());
            }
        };
    }
}
