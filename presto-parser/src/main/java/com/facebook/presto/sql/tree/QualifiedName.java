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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedName
{
    private final List<String> parts;
    private final List<String> originalParts;
    private final List<Boolean> isCaseSensitive;

    public static QualifiedName of(String first, String... rest)
    {
        requireNonNull(first, "first is null");
        return of(ImmutableList.copyOf(Lists.asList(first, rest)));
    }

    public static QualifiedName of(String name)
    {
        requireNonNull(name, "name is null");
        return of(ImmutableList.of(name));
    }

    public static QualifiedName of(Iterable<String> originalParts)
    {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        List<String> parts = ImmutableList.copyOf(transform(originalParts, part -> part.toLowerCase(ENGLISH)));
        ImmutableList.Builder<Boolean> builder = ImmutableList.builder();
        originalParts.forEach(x -> builder.add(false));

        return new QualifiedName(ImmutableList.copyOf(originalParts), parts, builder.build());
    }

    public static QualifiedName of(Iterable<String> originalParts, List<Boolean> isCaseSensitive)
    {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        List<String> parts = ImmutableList.copyOf(transform(originalParts, part -> part.toLowerCase(ENGLISH)));
        List<String> actualParts = ImmutableList.copyOf(originalParts);
        checkArgument(parts.size() == isCaseSensitive.size(), "Size didn't match");
        ImmutableList.Builder<Boolean> builder = ImmutableList.builder();
        for (int i = 0; i < parts.size(); i++) {
            builder.add(isCaseSensitive.get(i) && actualParts.get(i).equals(parts.get(i)));
        }

        return new QualifiedName(ImmutableList.copyOf(originalParts), parts, isCaseSensitive);
    }

    private QualifiedName(List<String> originalParts, List<String> parts, List<Boolean> isCaseSensitive)
    {
        this.originalParts = originalParts;
        this.parts = parts;
        this.isCaseSensitive = isCaseSensitive;
    }

    public List<String> getParts()
    {
        return parts;
    }

    public List<String> getOriginalParts()
    {
        return originalParts;
    }

    public List<Boolean> isCaseSensitive()
    {
        return isCaseSensitive;
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

        List<String> subList = parts.subList(0, parts.size() - 1);
        return Optional.of(new QualifiedName(subList, subList, isCaseSensitive));
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

    public String getOriginalSuffix()
    {
        return Iterables.getLast(isCaseSensitive) ? Iterables.getLast(originalParts) : Iterables.getLast(parts);
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
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }
}
