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
package com.facebook.presto.hive.metastore.file;

import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.spi.security.PrincipalType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DatabaseMetadata
{
    private final String ownerName;
    private final PrincipalType ownerType;
    private final Optional<String> comment;
    private final Map<String, String> parameters;

    @JsonCreator
    public DatabaseMetadata(
            @JsonProperty("ownerName") String ownerName,
            @JsonProperty("ownerType") PrincipalType ownerType,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.ownerName = requireNonNull(ownerName, "ownerName is null");
        this.ownerType = requireNonNull(ownerType, "ownerType is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    public DatabaseMetadata(Database database)
    {
        this.ownerName = database.getOwnerName();
        this.ownerType = database.getOwnerType();
        this.comment = database.getComment();
        this.parameters = database.getParameters();
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

    public Database toDatabase(String databaseName, String location)
    {
        return Database.builder()
                .setDatabaseName(databaseName)
                .setLocation(Optional.of(location))
                .setOwnerName(ownerName)
                .setOwnerType(ownerType)
                .setParameters(parameters)
                .build();
    }
}
