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

import com.facebook.presto.spi.security.PrincipalType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Database
{
    public static final String DEFAULT_DATABASE_NAME = "default";

    private final String databaseName;
    private final Optional<String> location;
    private final String ownerName;
    private final PrincipalType ownerType;
    private final Optional<String> comment;
    private final Map<String, String> parameters;

    @JsonCreator
    public Database(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("location") Optional<String> location,
            @JsonProperty("ownerName") String ownerName,
            @JsonProperty("ownerType") PrincipalType ownerType,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.location = requireNonNull(location, "location is null");
        this.ownerName = requireNonNull(ownerName, "ownerName is null");
        this.ownerType = requireNonNull(ownerType, "ownerType is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public Optional<String> getLocation()
    {
        return location;
    }

    @JsonProperty
    public String getOwnerName()
    {
        return ownerName;
    }

    @JsonProperty
    public PrincipalType getOwnerType()
    {
        return ownerType;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Database database)
    {
        return new Builder(database);
    }

    public static class Builder
    {
        private String databaseName;
        private Optional<String> location = Optional.empty();
        private String ownerName;
        private PrincipalType ownerType;
        private Optional<String> comment = Optional.empty();
        private Map<String, String> parameters = new LinkedHashMap<>();

        public Builder() {}

        public Builder(Database database)
        {
            this.databaseName = database.databaseName;
            this.location = database.location;
            this.ownerName = database.ownerName;
            this.ownerType = database.ownerType;
            this.comment = database.comment;
            this.parameters = database.parameters;
        }

        public Builder setDatabaseName(String databaseName)
        {
            requireNonNull(databaseName, "databaseName is null");
            this.databaseName = databaseName;
            return this;
        }

        public Builder setLocation(Optional<String> location)
        {
            requireNonNull(location, "location is null");
            this.location = location;
            return this;
        }

        public Builder setOwnerName(String ownerName)
        {
            requireNonNull(ownerName, "ownerName is null");
            this.ownerName = ownerName;
            return this;
        }

        public Builder setOwnerType(PrincipalType ownerType)
        {
            requireNonNull(ownerType, "ownerType is null");
            this.ownerType = ownerType;
            return this;
        }

        public Builder setComment(Optional<String> comment)
        {
            requireNonNull(comment, "comment is null");
            this.comment = comment;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            requireNonNull(parameters, "parameters is null");
            this.parameters = parameters;
            return this;
        }

        public Database build()
        {
            return new Database(
                    databaseName,
                    location,
                    ownerName,
                    ownerType,
                    comment,
                    parameters);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("databaseName", databaseName)
                .add("location", location)
                .add("ownerName", ownerName)
                .add("ownerType", ownerType)
                .add("comment", comment)
                .add("parameters", parameters)
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

        Database database = (Database) o;
        return Objects.equals(databaseName, database.databaseName) &&
                Objects.equals(location, database.location) &&
                Objects.equals(ownerName, database.ownerName) &&
                ownerType == database.ownerType &&
                Objects.equals(comment, database.comment) &&
                Objects.equals(parameters, database.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, location, ownerName, ownerType, comment, parameters);
    }
}
