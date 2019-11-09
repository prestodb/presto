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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.security.PrestoPrincipal;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class UserTableKey
{
    private final PrestoPrincipal principal;
    private final String database;
    private final String table;

    @JsonCreator
    public UserTableKey(
            @JsonProperty("principal") PrestoPrincipal principal,
            @JsonProperty("database") String database,
            @JsonProperty("table") String table)
    {
        // principal can be null when we want to list all privileges for admins
        this.principal = principal;
        this.table = requireNonNull(table, "table is null");
        this.database = requireNonNull(database, "database is null");
    }

    @JsonProperty
    public PrestoPrincipal getPrincipal()
    {
        return principal;
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    public boolean matches(String databaseName, String tableName)
    {
        return this.database.equals(databaseName) && this.table.equals(tableName);
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
        UserTableKey that = (UserTableKey) o;
        return Objects.equals(principal, that.principal) &&
                Objects.equals(table, that.table) &&
                Objects.equals(database, that.database);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(principal, table, database);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("principal", principal)
                .add("table", table)
                .add("database", database)
                .toString();
    }
}
