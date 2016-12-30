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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MongoTableHandle
        implements ConnectorTableHandle
{
    private final String databaseName; // case sensitive name
    private final String collectionName; // case sensitive name

    public MongoTableHandle(SchemaTableName table)
    {
        this(table.getSchemaName(), table.getTableName());
    }

    @JsonCreator
    public MongoTableHandle(@JsonProperty("databaseName") String databaseName,
                            @JsonProperty("collectionName") String collectionName)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.collectionName = requireNonNull(collectionName, "collectionName is null");
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(databaseName, collectionName);
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getCollectionName()
    {
        return collectionName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, collectionName);
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
        MongoTableHandle other = (MongoTableHandle) obj;
        return Objects.equals(this.databaseName, other.databaseName) &&
                Objects.equals(this.collectionName, other.collectionName);
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s", databaseName, collectionName);
    }
}
