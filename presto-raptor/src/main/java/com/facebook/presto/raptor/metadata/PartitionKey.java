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
package com.facebook.presto.raptor.metadata;

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

public final class PartitionKey
{
    private final String partitionName;
    private final String name;
    private final Type type;
    private final String value;

    @JsonCreator
    public PartitionKey(
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

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

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
        PartitionKey other = (PartitionKey) obj;
        return Objects.equal(this.partitionName, other.partitionName) &&
                Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.value, other.value);
    }

    public static class Mapper
            implements ResultSetMapper<PartitionKey>
    {
        private final TypeManager typeManager;

        @Inject
        public Mapper(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public PartitionKey map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new PartitionKey(r.getString("partition_name"),
                    r.getString("key_name"),
                    typeManager.getType(r.getString("key_type")),
                    r.getString("key_value"));
        }
    }

    public static Predicate<PartitionKey> partitionNamePredicate(final String partitionName)
    {
        checkNotNull(partitionName, "partitionName is null");

        return new Predicate<PartitionKey>()
        {
            @Override
            public boolean apply(PartitionKey input)
            {
                return partitionName.equals(input.getPartitionName());
            }
        };
    }
}
