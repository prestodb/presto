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
package com.facebook.presto.metadata;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ViewDefinition
{
    private final String originalSql;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<ViewColumn> columns;
    private final Optional<String> owner;
    private final boolean runAsInvoker;

    @JsonCreator
    public ViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("columns") List<ViewColumn> columns,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("runAsInvoker") boolean runAsInvoker)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.owner = requireNonNull(owner, "owner is null");
        this.runAsInvoker = runAsInvoker;
        checkArgument(!runAsInvoker || !owner.isPresent(), "owner cannot be present with runAsInvoker");
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<ViewColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @JsonProperty
    public boolean isRunAsInvoker()
    {
        return runAsInvoker;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("originalSql", originalSql)
                .add("catalog", catalog.orElse(null))
                .add("schema", schema.orElse(null))
                .add("columns", columns)
                .add("owner", owner.orElse(null))
                .add("runAsInvoker", runAsInvoker)
                .omitNullValues()
                .toString();
    }

    public ViewDefinition withOwner(String owner)
    {
        return new ViewDefinition(originalSql, catalog, schema, columns, Optional.of(owner), runAsInvoker);
    }

    public static final class ViewColumn
    {
        private final String name;
        private final Type type;

        @JsonCreator
        public ViewColumn(
                @JsonProperty("name") String name,
                @JsonProperty("type") Type type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return name + ":" + type;
        }
    }
}
