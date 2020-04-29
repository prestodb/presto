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
package com.facebook.presto.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Subfield
{
    public interface PathElement
    {
        boolean isSubscript();
    }

    public static final class AllSubscripts
            implements PathElement
    {
        private static final AllSubscripts ALL_SUBSCRIPTS = new AllSubscripts();

        private AllSubscripts() {}

        public static AllSubscripts getInstance()
        {
            return ALL_SUBSCRIPTS;
        }

        @Override
        public boolean isSubscript()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "[*]";
        }
    }

    public static final class NestedField
            implements PathElement
    {
        private final String name;

        public NestedField(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

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

    public static final class LongSubscript
            implements PathElement
    {
        private final long index;

        public LongSubscript(long index)
        {
            this.index = index;
        }

        public long getIndex()
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

            LongSubscript that = (LongSubscript) o;
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
            implements PathElement
    {
        private final String index;

        public StringSubscript(String index)
        {
            this.index = requireNonNull(index, "index is null");
        }

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
            return "[\"" + index.replace("\"", "\\\"") + "\"]";
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
        return AllSubscripts.getInstance();
    }

    @JsonCreator
    public Subfield(String path)
    {
        requireNonNull(path, "path is null");

        SubfieldTokenizer tokenizer = new SubfieldTokenizer(path);
        checkArgument(tokenizer.hasNext(), "Column name is missing: " + path);

        PathElement firstElement = tokenizer.next();
        checkArgument(firstElement instanceof NestedField, "Subfield path must start with a name: " + path);

        this.name = ((NestedField) firstElement).getName();

        List<PathElement> pathElements = new ArrayList<>();
        tokenizer.forEachRemaining(pathElements::add);
        this.path = Collections.unmodifiableList(pathElements);
    }

    private static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public Subfield(String name, List<PathElement> path)
    {
        this.name = requireNonNull(name, "name is null");
        this.path = requireNonNull(path, "path is null");
    }

    public String getRootName()
    {
        return name;
    }

    public List<PathElement> getPath()
    {
        return path;
    }

    public boolean isPrefix(Subfield other)
    {
        if (!other.name.equals(name)) {
            return false;
        }

        if (path.size() < other.path.size()) {
            return Objects.equals(path, other.path.subList(0, path.size()));
        }

        return false;
    }

    public Subfield tail(String name)
    {
        if (path.isEmpty()) {
            throw new IllegalStateException("path is empty");
        }

        return new Subfield(name, path.subList(1, path.size()));
    }

    @JsonValue
    public String serialize()
    {
        return name + path.stream()
                .map(PathElement::toString)
                .collect(Collectors.joining());
    }

    @Override
    public String toString()
    {
        return serialize();
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

        Subfield other = (Subfield) o;
        return Objects.equals(name, other.name) &&
                Objects.equals(path, other.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, path);
    }
}
