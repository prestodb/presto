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
package com.facebook.presto.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A node of the column tree assembled by {@link SchemaBuilder} out of the flat {@code
 * ducklake_column} rows of a table visible at a snapshot. Mirrors Iceberg's {@code ColumnIdentity}
 * but keys columns by the {@code long} DuckLake column id and keeps the raw DuckLake catalog type
 * string ({@code duckLakeType}, for example {@code int32}, {@code decimal(18,3)}, {@code list})
 * instead of a format-specific type object, since the type-unit information some DuckLake types
 * carry (for example {@code timestamp_ns} vs {@code timestamp}, {@code uint8} vs {@code int16}) is
 * lost once the column is converted to a Presto {@link com.facebook.presto.common.type.Type} by
 * {@link TypeConverter}. Primitives have no children; a {@code list} has exactly one child (the
 * element); a {@code map} has exactly two children (key then value); a {@code struct} has its
 * fields in {@code column_order}.
 */
public class DuckLakeColumnIdentity
{
    private final long id;
    private final String name;
    private final TypeCategory typeCategory;
    private final String duckLakeType;
    private final List<DuckLakeColumnIdentity> children;

    @JsonCreator
    public DuckLakeColumnIdentity(
            @JsonProperty("id") long id,
            @JsonProperty("name") String name,
            @JsonProperty("typeCategory") TypeCategory typeCategory,
            @JsonProperty("duckLakeType") String duckLakeType,
            @JsonProperty("children") List<DuckLakeColumnIdentity> children)
    {
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.typeCategory = requireNonNull(typeCategory, "typeCategory is null");
        this.duckLakeType = requireNonNull(duckLakeType, "duckLakeType is null");
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
        checkArgument(
                this.children.isEmpty() == (typeCategory == TypeCategory.PRIMITIVE),
                "Children should be empty if and only if column type is primitive");
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TypeCategory getTypeCategory()
    {
        return typeCategory;
    }

    @JsonProperty
    public String getDuckLakeType()
    {
        return duckLakeType;
    }

    @JsonProperty
    public List<DuckLakeColumnIdentity> getChildren()
    {
        return children;
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
        DuckLakeColumnIdentity that = (DuckLakeColumnIdentity) o;
        return id == that.id &&
                name.equals(that.name) &&
                typeCategory == that.typeCategory &&
                duckLakeType.equals(that.duckLakeType) &&
                children.equals(that.children);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, name, typeCategory, duckLakeType, children);
    }

    @Override
    public String toString()
    {
        return id + ":" + name + ":" + typeCategory + ":" + duckLakeType + ":" + children;
    }

    public enum TypeCategory
    {
        PRIMITIVE,
        STRUCT,
        ARRAY,
        MAP
    }
}
