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
package io.prestosql.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class HiveTableName
{
    private final String databaseName;
    private final String tableName;

    @JsonCreator
    public HiveTableName(@JsonProperty("databaseName") String databaseName, @JsonProperty("tableName") String tableName)
    {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static HiveTableName hiveTableName(String databaseName, String tableName)
    {
        return new HiveTableName(databaseName, tableName);
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("databaseName", databaseName)
                .add("tableName", tableName)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiveTableName other = (HiveTableName) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, tableName);
    }
}
