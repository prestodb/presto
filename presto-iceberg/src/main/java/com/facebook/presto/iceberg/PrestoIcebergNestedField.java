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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PrestoIcebergNestedField
{
    private final boolean optional;
    private final int id;
    private final String name;
    private final Type prestoType;
    private final Optional<String> doc;

    @JsonCreator
    public PrestoIcebergNestedField(
            @JsonProperty("optional") boolean optional,
            @JsonProperty("id") int id,
            @JsonProperty("name") String name,
            @JsonProperty("prestoType") Type prestoType,
            @JsonProperty("doc") Optional<String> doc)
    {
        this.optional = optional;
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.prestoType = requireNonNull(prestoType, "prestoType is null");
        this.doc = requireNonNull(doc, "doc is null");
    }

    @JsonProperty
    public boolean isOptional()
    {
        return optional;
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getPrestoType()
    {
        return prestoType;
    }

    @JsonProperty
    public Optional<String> getDoc()
    {
        return doc;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(optional, id, name, prestoType, doc);
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

        PrestoIcebergNestedField other = (PrestoIcebergNestedField) obj;
        return Objects.equals(this.optional, other.optional) &&
                Objects.equals(this.id, other.id) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.prestoType, other.prestoType) &&
                Objects.equals(this.doc, other.doc);
    }
}
