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
import static java.util.Objects.requireNonNull;

@Immutable
public class UserDatabaseKey
{
    private final String user;
    private final String database;

    @JsonCreator
    public UserDatabaseKey(@JsonProperty("user") String user, @JsonProperty("database") String database)
    {
        this.user = requireNonNull(user, "principalName is null");
        this.database = requireNonNull(database, "database is null");
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
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
        UserDatabaseKey that = (UserDatabaseKey) o;
        return Objects.equals(user, that.user) &&
                Objects.equals(database, that.database);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user, database);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("principalName", user)
                .add("database", database)
                .toString();
    }
}
