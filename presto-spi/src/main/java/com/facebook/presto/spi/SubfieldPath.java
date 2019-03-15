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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class SubfieldPath
{
    private static final PathElement ALL_SUBSCRIPTS = new IntegerSubscript(-1);

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = NestedField.class, name = "nestedField"),
            @JsonSubTypes.Type(value = IntegerSubscript.class, name = "integerSubscript"),
            @JsonSubTypes.Type(value = StringSubscript.class, name = "stringSubscript"),
    })
    public abstract static class PathElement
    {
        public abstract boolean isSubscript();
    }

    public static final class NestedField
            extends PathElement
    {
        private final String name;

        @JsonCreator
        public NestedField(@JsonProperty("name") String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
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

            NestedField that = (NestedField) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return "." + name;
        }

        @Override
        public boolean isSubscript()
        {
            return false;
        }
    }

    public static final class IntegerSubscript
            extends PathElement
    {
        private final int index;

        @JsonCreator
        public IntegerSubscript(@JsonProperty("index") int index)
        {
            this.index = index;
        }

        @JsonProperty
        public int getIndex()
        {
            return index;
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

            IntegerSubscript that = (IntegerSubscript) o;
            return index == that.index;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(index);
        }

        @Override
        public String toString()
        {
            return "[" + index + "]";
        }

        @Override
        public boolean isSubscript()
        {
            return true;
        }
    }

    public static final class StringSubscript
            extends PathElement
    {
        private final String index;

        @JsonCreator
        public StringSubscript(@JsonProperty("index") String index)
        {
            this.index = requireNonNull(index, "index is null");
        }

        @JsonProperty
        public String getIndex()
        {
            return index;
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

            StringSubscript that = (StringSubscript) o;
            return Objects.equals(index, that.index);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(index);
        }

        @Override
        public String toString()
        {
            return "[" + index + "]";
        }

        @Override
        public boolean isSubscript()
        {
            return true;
        }
    }

    private final String name;
    private final List<PathElement> path;

    public static PathElement allSubscripts()
    {
        return ALL_SUBSCRIPTS;
    }

    // TODO Add column name as a separate argument and remove it from the path
    @JsonCreator
    public SubfieldPath(@JsonProperty("path") List<PathElement> path)
    {
        requireNonNull(path, "path is null");
        if (path.size() <= 1) {
            throw new IllegalArgumentException("path must include at least 2 elements");
        }
        if (!(path.get(0) instanceof NestedField)) {
            throw new IllegalArgumentException("path must start with a name");
        }
        this.name = ((NestedField) path.get(0)).getName();
        this.path = path;
    }

    public String getColumnName()
    {
        return name;
    }

    @JsonProperty("path")
    public List<PathElement> getPath()
    {
        return path;
    }

    @Override
    public String toString()
    {
        return path.stream().map(PathElement::toString).collect(Collectors.joining());
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

        SubfieldPath other = (SubfieldPath) o;
        return Objects.equals(path, other.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(path);
    }
}
