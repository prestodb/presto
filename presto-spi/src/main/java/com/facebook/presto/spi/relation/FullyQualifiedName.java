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
package com.facebook.presto.spi.relation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FullyQualifiedName
{
    private final List<String> parts;
    private final List<String> originalParts;

    @JsonCreator
    public static FullyQualifiedName of(String dottedName)
    {
        String[] parts = dottedName.split("\\.");
        if (parts.length < 3) {
            throw new IllegalArgumentException("FullyQualifiedName should be in the form of 'a.b.c' and have at least 3 parts");
        }
        return of(asList(parts));
    }

    public static FullyQualifiedName of(String part1, String part2, String part3, String... rest)
    {
        List<String> parts = new ArrayList<>(rest.length + 3);
        parts.add(part1);
        parts.add(part2);
        parts.add(part3);
        parts.addAll(asList(rest));
        return of(parts);
    }

    public static FullyQualifiedName of(List<String> originalParts)
    {
        requireNonNull(originalParts, "originalParts is null");
        if (originalParts.size() < 3) {
            throw new IllegalArgumentException("originalParts should have at least 3 parts");
        }

        List<String> parts = new ArrayList<>(originalParts.size());
        for (String originalPart : originalParts) {
            parts.add(originalPart.toLowerCase(ENGLISH));
        }

        return new FullyQualifiedName(originalParts, parts);
    }

    public static FullyQualifiedName of(FullyQualifiedName.Prefix prefix, String name)
    {
        List<String> parts = new ArrayList<>(prefix.parts);
        parts.add(name);
        return of(parts);
    }

    private FullyQualifiedName(List<String> originalParts, List<String> parts)
    {
        this.originalParts = unmodifiableList(originalParts);
        this.parts = unmodifiableList(parts);
    }

    public List<String> getParts()
    {
        return parts;
    }

    public List<String> getOriginalParts()
    {
        return originalParts;
    }

    public String getSuffix()
    {
        return parts.get(parts.size() - 1);
    }

    public Prefix getPrefix()
    {
        return new Prefix(parts.subList(0, parts.size() - 1));
    }

    @JsonValue
    @Override
    public String toString()
    {
        return String.join(".", parts);
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
        return parts.equals(((FullyQualifiedName) o).parts);
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }

    public static class Prefix
    {
        private final List<String> parts;

        private Prefix(List<String> parts)
        {
            this.parts = unmodifiableList(parts);
        }

        public static Prefix of(String dottedName)
        {
            String[] parts = dottedName.split("\\.");
            if (parts.length < 2) {
                throw new IllegalArgumentException("Prefix should be in the form of a.b(.c...) with at least 1 dot");
            }
            return new Prefix(asList(parts));
        }

        public boolean contains(Prefix other)
        {
            if (parts.size() > other.parts.size()) {
                return false;
            }
            for (int i = 0; i < parts.size(); i++) {
                if (!parts.get(i).equals(other.parts.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString()
        {
            return String.join(".", parts);
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
            return parts.equals(((FullyQualifiedName.Prefix) o).parts);
        }

        @Override
        public int hashCode()
        {
            return parts.hashCode();
        }
    }
}
