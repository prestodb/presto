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
package com.facebook.presto.sql.tree;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedName
{
    private final List<String> parts;

    public static QualifiedName of(QualifiedName prefix, String suffix)
    {
        requireNonNull(prefix, "prefix is null");
        requireNonNull(suffix, "suffix is null");

        return new QualifiedName(Iterables.concat(prefix.getParts(), ImmutableList.of(suffix)));
    }

    public static QualifiedName of(String prefix, QualifiedName suffix)
    {
        requireNonNull(prefix, "prefix is null");
        requireNonNull(suffix, "suffix is null");

        return QualifiedName.of(Iterables.concat(ImmutableList.of(prefix), suffix.getParts()));
    }

    public static QualifiedName of(String first, String... rest)
    {
        requireNonNull(first, "first is null");
        return new QualifiedName(ImmutableList.copyOf(Lists.asList(first, rest)));
    }

    public static QualifiedName of(Iterable<String> parts)
    {
        requireNonNull(parts, "parts is null");
        Preconditions.checkArgument(!Iterables.isEmpty(parts), "parts is empty");

        return new QualifiedName(parts);
    }

    public static QualifiedName parseQualifiedName(String qualifiedName)
    {
        requireNonNull(qualifiedName, "qualifiedName is null");

        return of(Splitter.on('.').split(qualifiedName));
    }

    public QualifiedName(String name)
    {
        this(ImmutableList.of(name));
    }

    public QualifiedName(Iterable<String> parts)
    {
        requireNonNull(parts, "parts");
        Preconditions.checkArgument(!Iterables.isEmpty(parts), "parts is empty");

        this.parts = ImmutableList.copyOf(Iterables.transform(parts, s -> s.toLowerCase(ENGLISH)));
    }

    public List<String> getParts()
    {
        return parts;
    }

    @Override
    public String toString()
    {
        return Joiner.on('.').join(parts);
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix()
    {
        if (parts.size() == 1) {
            return Optional.empty();
        }

        return Optional.of(QualifiedName.of(parts.subList(0, parts.size() - 1)));
    }

    public boolean hasSuffix(QualifiedName suffix)
    {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int start = parts.size() - suffix.getParts().size();

        return parts.subList(start, parts.size()).equals(suffix.getParts());
    }

    public String getSuffix()
    {
        return Iterables.getLast(parts);
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

        QualifiedName that = (QualifiedName) o;

        if (!parts.equals(that.parts)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }
}
