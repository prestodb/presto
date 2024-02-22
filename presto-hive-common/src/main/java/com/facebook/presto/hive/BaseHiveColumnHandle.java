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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BaseHiveColumnHandle
        implements ColumnHandle
{
    public enum ColumnType
    {
        PARTITION_KEY,
        REGULAR,
        SYNTHESIZED,
        AGGREGATED,
    }

    private final String name;
    private final Optional<String> comment;
    private final ColumnType columnType;
    private final List<Subfield> requiredSubfields;

    @JsonCreator
    public BaseHiveColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("requiredSubfields") List<Subfield> requiredSubfields)
    {
        this.name = requireNonNull(name, "name is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.requiredSubfields = requireNonNull(requiredSubfields, "requiredSubfields is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public List<Subfield> getRequiredSubfields()
    {
        return requiredSubfields;
    }
}
