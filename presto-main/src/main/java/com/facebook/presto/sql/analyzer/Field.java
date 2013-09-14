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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

public class Field
{
    private final Optional<QualifiedName> relationAlias;
    private final Optional<String> name;
    private final Type type;

    public static Field newUnqualified(String name, Type type)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(type, "type is null");

        return new Field(Optional.<QualifiedName>absent(), Optional.of(name), type);
    }

    public static Field newUnqualified(Optional<String> name, Type type)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(type, "type is null");

        return new Field(Optional.<QualifiedName>absent(), name, type);
    }

    public static Field newQualified(QualifiedName relationAlias, Optional<String> name, Type type)
    {
        Preconditions.checkNotNull(relationAlias, "relationAlias is null");
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(type, "type is null");

        return new Field(Optional.of(relationAlias), name, type);
    }

    private Field(Optional<QualifiedName> relationAlias, Optional<String> name, Type type)
    {
        checkNotNull(relationAlias, "relationAlias is null");
        checkNotNull(name, "name is null");
        checkNotNull(type, "type is null");

        this.relationAlias = relationAlias;
        this.name = name;
        this.type = type;
    }

    public Optional<QualifiedName> getRelationAlias()
    {
        return relationAlias;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public static Function<Field, Type> typeGetter()
    {
        return new Function<Field, Type>()
        {
            @Override
            public Type apply(Field field)
            {
                return field.getType();
            }
        };
    }

    public boolean matchesPrefix(Optional<QualifiedName> prefix)
    {
        return !prefix.isPresent() || relationAlias.isPresent() && relationAlias.get().hasSuffix(prefix.get());
    }

    /*
      Namespaces can have names such as "x", "x.y" or "" if there's no name
      Name to resolve can have names like "a", "x.a", "x.y.a"

      namespace  name     possible match
       ""         "a"           y
       "x"        "a"           y
       "x.y"      "a"           y

       ""         "x.a"         n
       "x"        "x.a"         y
       "x.y"      "x.a"         n

       ""         "x.y.a"       n
       "x"        "x.y.a"       n
       "x.y"      "x.y.a"       n

       ""         "y.a"         n
       "x"        "y.a"         n
       "x.y"      "y.a"         y
     */
    public boolean canResolve(QualifiedName name)
    {
        if (!this.name.isPresent()) {
            return false;
        }

        // TODO: need to know whether the qualified name and the name of this field were quoted
        return matchesPrefix(name.getPrefix()) && this.name.get().equalsIgnoreCase(name.getSuffix());
    }

    public static Function<Field, Optional<QualifiedName>> relationAliasGetter()
    {
        return new Function<Field, Optional<QualifiedName>>()
        {
            @Override
            public Optional<QualifiedName> apply(Field input)
            {
                return input.getRelationAlias();
            }
        };
    }

    public static Predicate<Field> canResolvePredicate(final QualifiedName name)
    {
        return new Predicate<Field>()
        {
            @Override
            public boolean apply(Field input)
            {
                return input.canResolve(name);
            }
        };
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        if (relationAlias.isPresent()) {
            result.append(relationAlias.get())
                    .append(".");
        }

        result.append(name.or("<anonymous>"))
                .append(":")
                .append(type);

        return result.toString();
    }
}
