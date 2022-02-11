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

import com.facebook.presto.hive.HiveType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Column
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Column.class).instanceSize();

    private final String name;
    private final HiveType type;
    private final Optional<String> comment;
    private final Optional<String> typeMetadata;

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") HiveType type,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("typeMetadata") Optional<String> typeMetadata)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.typeMetadata = requireNonNull(typeMetadata, "typeMetadata is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public HiveType getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Optional<String> getTypeMetadata()
    {
        return typeMetadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
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

        Column column = (Column) o;
        return Objects.equals(name, column.name) &&
                Objects.equals(type, column.type) &&
                Objects.equals(comment, column.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment);
    }

    public int getEstimatedSizeInBytes()
    {
        int result = INSTANCE_SIZE;
        result += name.length() * Character.BYTES;
        result += type.getEstimatedRetainedSizeInBytes();
        result += comment.map(String::length).orElse(0);
        return result;
    }
}
