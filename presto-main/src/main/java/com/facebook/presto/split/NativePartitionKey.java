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
package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.PartitionKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class NativePartitionKey
        implements PartitionKey
{
    private final String partitionName;
    private final String name;
    private final ColumnType type;
    private final String value;

    @JsonCreator
    public NativePartitionKey(
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("name") String name,
            @JsonProperty("type") ColumnType type,
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
    public ColumnType getType()
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
        final NativePartitionKey other = (NativePartitionKey) obj;
        return Objects.equal(this.partitionName, other.partitionName) &&
                Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.value, other.value);
    }

    public static class Mapper
            implements ResultSetMapper<NativePartitionKey>
    {
        @Override
        public NativePartitionKey map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new NativePartitionKey(r.getString("partition_name"),
                    r.getString("key_name"),
                    ColumnType.valueOf(r.getString("key_type")),
                    r.getString("key_value"));
        }
    }

    public static Predicate<NativePartitionKey> partitionNamePredicate(final String partitionName)
    {
        checkNotNull(partitionName, "partitionName is null");

        return new Predicate<NativePartitionKey>()
        {
            @Override
            public boolean apply(NativePartitionKey input)
            {
                return partitionName.equals(input.getPartitionName());
            }
        };
    }
}
