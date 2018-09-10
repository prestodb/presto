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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Symbol
        implements Comparable<Symbol>
{
    private final String name;
    private final Set<String> fields;

    public static Symbol from(Expression expression)
    {
        checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
        return new Symbol(((SymbolReference) expression).getName(), ((SymbolReference) expression).getFields());
    }

    public static Symbol withField(DereferenceExpression expression)
    {
        checkArgument(expression.getBase() instanceof SymbolReference, "Unexpected base expression: %s", expression.getBase());
        return new Symbol(((SymbolReference) expression.getBase()).getName(), ImmutableSet.of(expression.getField().getValue()));
    }

    public static Symbol withAllFields(SymbolReference expression)
    {
        return new Symbol(expression.getName(), expression.getFields());
    }

    public static Symbol withEmptyFields(Symbol symbol)
    {
        return new Symbol(symbol.getName());
    }

    public static Symbol withEmptyFields(SymbolReference symbolReference)
    {
        return new Symbol(symbolReference.getName());
    }

    public static Symbol merge(Symbol a, Symbol b)
    {
        checkArgument(a.getName().equalsIgnoreCase(b.getName()), "Symbols with different names cannot be merged: %s vs %s", a.getName(), b.getName());
        return new Symbol(a.getName(), mergeAndPruneFields(a.fields, b.fields));
    }

    public static boolean contains(Symbol a, Symbol b)
    {
        if (!a.getName().equalsIgnoreCase(b.getName())) {
            return false;
        }

        if (!a.getFields().isEmpty() && !a.getFields().containsAll(b.getFields())) {
            return false;
        }

        return true;
    }

    public Symbol(String name)
    {
        this(name, ImmutableSet.of());
    }

    @JsonCreator
    public Symbol(
            @JsonProperty("name") String name,
            @JsonProperty("fields") Set<String> fields)
    {
        requireNonNull(name, "name is null");
        requireNonNull(fields, "fields is null");
        this.name = name;
        this.fields = fields;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Set<String> getFields()
    {
        return fields;
    }

    public SymbolReference toSymbolReference()
    {
        return new SymbolReference(name, fields);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("fields", fields)
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

        Symbol symbol = (Symbol) o;

        if (!name.equals(symbol.name)) {
            return false;
        }

        if (!fields.equals(symbol.fields)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fields);
    }

    @Override
    public int compareTo(Symbol o)
    {
        return name.compareTo(o.name);
    }

    private static Set<String> mergeAndPruneFields(Set<String> a, Set<String> b)
    {
        if (a.size() == 0 || b.size() == 0) {
            return ImmutableSet.of();
        }
        return ImmutableSet.<String>builder()
                .addAll(a)
                .addAll(b)
                .build();
    }

    public static class SymbolKeySerializer
            extends JsonSerializer<Symbol>
    {
        private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();

        @Override
        public void serialize(Symbol symbol, JsonGenerator jsonGenerator, SerializerProvider serializers)
                throws IOException
        {
            jsonGenerator.writeFieldName(MAPPER.writeValueAsString(symbol));
        }
    }

    public static class SymbolKeyDeserializer
            extends KeyDeserializer
    {
        private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();

        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt)
                throws IOException
        {
            return MAPPER.readValue(key, Symbol.class);
        }
    }
}
