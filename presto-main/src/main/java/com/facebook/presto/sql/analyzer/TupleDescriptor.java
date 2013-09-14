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
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.Field.relationAliasGetter;
import static com.facebook.presto.sql.analyzer.Optionals.isPresentPredicate;

public class TupleDescriptor
{
    private final List<Field> fields;

    public TupleDescriptor(Field... fields)
    {
        this(Arrays.asList(fields));
    }

    public TupleDescriptor(List<Field> fields)
    {
        Preconditions.checkNotNull(fields, "fields is null");
        this.fields = ImmutableList.copyOf(fields);
    }

    public List<Field> getFields()
    {
        return fields;
    }

    public Set<QualifiedName> getRelationAliases()
    {
        return IterableTransformer.on(fields)
                .transform(relationAliasGetter())
                .select(isPresentPredicate())
                .transform(Optionals.<QualifiedName>optionalGetter())
                .set();
    }

    public boolean canResolve(QualifiedName name)
    {
        return Iterables.any(fields, Field.canResolvePredicate(name));
    }

    public List<Field> resolveFieldsWithPrefix(Optional<QualifiedName> prefix)
    {
        ImmutableList.Builder<Field> builder = ImmutableList.builder();

        for (Field field : fields) {
            if (field.matchesPrefix(prefix)) {
                builder.add(field);
            }
        }

        return builder.build();
    }

    public List<Integer> resolveFieldIndexesWithPrefix(Optional<QualifiedName> prefix)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();

        int index = 0;
        for (Field field : fields) {
            if (field.matchesPrefix(prefix)) {
                builder.add(index);
            }
            index++;
        }

        return builder.build();
    }

    public List<Integer> resolveFieldIndexes(QualifiedName name)
    {
        ImmutableList.Builder<Integer> fields = ImmutableList.builder();

        for (int index = 0; index < this.fields.size(); index++) {
            Field field = this.fields.get(index);

            if (field.canResolve(name)) {
                fields.add(index);
            }
        }

        return fields.build();
    }

    public Predicate<QualifiedName> canResolvePredicate()
    {
        return new Predicate<QualifiedName>()
        {
            @Override
            public boolean apply(QualifiedName input)
            {
                return canResolve(input);
            }
        };
    }

    @Override
    public String toString()
    {
        return fields.toString();
    }
}
